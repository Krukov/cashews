import asyncio
import math
from collections import namedtuple
from functools import wraps
from typing import Any, Iterable, Optional, Tuple, Union

from cashews._typing import AsyncCallable_T, Decorator, KeyOrTemplate
from cashews.backends.interface import _BackendInterface
from cashews.key import get_cache_key, get_cache_key_template
from cashews.utils import get_indexes

__all__ = ("bloom",)

TrueFalsePair = Tuple[int, int]
IntOrPair = Union[int, TrueFalsePair]
BloomParams = namedtuple("BloomParams", ("size", "number_of_buckets"))

possible_set = all


def all_zeros(values: Iterable[int]) -> bool:
    return all(v == 0 for v in values)


def not_set(values: Iterable[int]) -> bool:
    return not all(values)


def bloom(
    backend: _BackendInterface,
    *,
    capacity: int,
    name: Optional[KeyOrTemplate] = None,
    false_positives: Union[float, int] = 1,
    check_false_positive: bool = True,
    prefix: str = "bloom",
) -> Decorator:
    """
    Decorator that can help you to use bloom filter algorithm

    @bloom(name="user_name:{name}", false_positives=1, capacity=10_000)
    async def is_user_exists(name) -> bool:
        return name in ....

    :param backend: cache backend
    :param name: custom cache key
    :param capacity: the same as n - number of elements
    :param false_positives: Percents of false positive results
    :param check_false_positive: do we need to check if we have positive result
    :param prefix: custom prefix for key, default 'bloom'
    """
    assert false_positives and capacity
    assert 0 < false_positives < 100
    index_size, number_of_buckets = params_for(capacity, false_positives / 100)

    def _decor(func: AsyncCallable_T) -> AsyncCallable_T:
        _name = get_cache_key_template(func, key=name)
        _cache_key = f"{_name}:{index_size}"
        if prefix:
            _cache_key = f"{prefix}:{_cache_key}"

        _set = getattr(func, "set", None)

        async def __set(*args: Any, **kwargs: Any):
            if _set is None:
                result = await func(*args, **kwargs)
            else:
                result = await _set(*args, **kwargs)
            if not result:
                return result
            _bloom_key = get_cache_key(func, _name, args, kwargs)
            indexes = get_indexes(_bloom_key, number_of_buckets, index_size)
            await backend.incr_bits(_cache_key, *indexes)
            return result

        func.set = __set

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _bloom_key = get_cache_key(func, _name, args, kwargs)
            hashes = get_indexes(_bloom_key, number_of_buckets, index_size)
            values = await backend.get_bits(_cache_key, *hashes)
            if values is None:
                return await func(*args, **kwargs)
            if possible_set(values):  # if all bits is set
                # false positive
                if check_false_positive:
                    return await func(*args, **kwargs)
                return True
            return False

        return _wrap

    return _decor


def dual_bloom(
    backend: _BackendInterface,
    *,
    capacity: IntOrPair,
    name: Optional[KeyOrTemplate] = None,
    false: Optional[IntOrPair] = 1,
    no_collisions: bool = False,
    prefix: str = "dual_bloom",
) -> Decorator:
    """
    Decorator that can help you to use bloom filter algorithm
     this is  implementation with 2 bloom filters - one for true and another for false
     That give as ability use bloom filter without pre-filling data
     but with possible false positive and false negative results

    @dual_bloom(name="user_name:{name}", false=1, capacity=10_000)
    async def is_user_exists(name) -> bool:
        return name in ....

    :param backend: cache backend
    :param capacity: the same as n - number of elements
    :param name: custom cache key
    :param no_collisions: add value only no collisions
    :param false: Percents of false results
    :param prefix: custom prefix for key, default 'dual_bloom'
    """
    filters_params = _get_params_for_filters(false, capacity)

    def _decor(func: AsyncCallable_T) -> AsyncCallable_T:
        _cache_key = get_cache_key_template(func, key=name)
        if prefix:
            _cache_key = f"{prefix}:{_cache_key}"
        _true_bloom_key = _cache_key + ":true"
        _false_bloom_key = _cache_key + ":false"

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _bloom_key = get_cache_key(func, _cache_key, args, kwargs)
            indexes_true, indexes_false = _get_indexes(_bloom_key, *filters_params)

            true_values, false_values = await asyncio.gather(
                backend.get_bits(_true_bloom_key, *indexes_true),
                backend.get_bits(_false_bloom_key, *indexes_false),
            )
            if not_set(true_values) and not_set(false_values):
                # not set yet
                result = await func(*args, **kwargs)
                if result and (not no_collisions or all_zeros(true_values)):
                    await backend.incr_bits(_true_bloom_key, *indexes_true)
                if not result and (not no_collisions or all_zeros(false_values)):
                    await backend.incr_bits(_false_bloom_key, *indexes_false)
                return result
            if not_set(true_values) and possible_set(false_values):
                return False  # can be false Negative
            if not_set(false_values) and possible_set(true_values):
                return True  # can be false Positive
            return await func(*args, **kwargs)

        return _wrap

    return _decor


def _get_params_for_filters(
    false: Optional[IntOrPair] = 1,
    capacity: Optional[IntOrPair] = None,
) -> Tuple[BloomParams, BloomParams]:
    assert false and capacity
    assert 0 < false < 100
    capacity_true, capacity_false = _to_pair(capacity)
    false_true, false_false = _to_pair(false)
    return params_for(capacity_true, false_true / 100), params_for(capacity_false, false_false / 100)


def _to_pair(value: IntOrPair) -> TrueFalsePair:
    if isinstance(value, int):
        return value, value
    return value


def _get_indexes(key: str, params_true: BloomParams, params_false: BloomParams):
    index_size_true, number_of_buckets_true = params_true
    index_size_false, number_of_buckets_false = params_false
    return (
        get_indexes(key + "true", number_of_buckets_true, index_size_true),
        get_indexes(key + "false", number_of_buckets_false, index_size_false),
    )


def params_for(capacity: int, false_positives: float = 0.01) -> BloomParams:
    """
    capacity = n
    k=(m * ln 2 / n)
    m = k * n / ln 2
    p = (1-e^{-k * n/m})^{k}
    return m - number of bits in the array and k the number of hash functions
    """
    m = _count_m(capacity, false_positives)
    k = _count_k(m, capacity)
    assert k > 0, "too high false positive value"
    return m, k


def _count_k(m, n):
    return int(round((m / n) * math.log(2)))


def _count_k_from_p(p):
    return int(math.ceil(math.log(1.0 / p, 2)))


def _count_m(n, p=0.1):
    return int(math.ceil(-1 * n * math.log(p) / math.log(2) ** 2))


def _count_probability(n, m, k):
    ev = -(k * n / m)
    ome = 1 - math.e**ev
    return math.pow(ome, k)
