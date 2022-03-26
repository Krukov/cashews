import asyncio
import math
import warnings
from functools import wraps
from typing import Optional, Tuple, Union

from cashews.utils import get_hashes

from ..backends.interface import Backend
from ..key import get_cache_key, get_cache_key_template

__all__ = ("bloom",)

all_not_zeros = all
all_zeros = lambda values: all((v == 0 for v in values))
at_least_one_zero = lambda values: not all(values)


def bloom(
    backend: Backend,
    name: Optional[str] = None,
    index_size: Optional[int] = None,
    number_of_hashes: Optional[int] = None,
    false_positives: Optional[Union[float, int]] = 1,
    capacity: Optional[int] = None,
    check_false_positive: bool = True,
    prefix: str = "bloom",
):
    """
    Decorator that can help you to use bloom filter algorithm

    @bloom(name="user_name:{name}", false_positives=1, capacity=10_000)
    async def is_user_exists(name):
        return name in ....

    :param backend: cache backend
    :param name: custom cache key
    :param index_size: size of bloom filter
    :param number_of_hashes: the same as k
    :param capacity: the same as n - number of elements
    :param false_positives: Percents of false positive results
    :param check_false_positive: do we need to check if we have positive result
    :param prefix: custom prefix for key, default 'bloom'
    """

    if backend.name == "mem":
        warnings.warn("Remember that you should fill a bloom filter before using it.")

    if index_size is None:
        assert false_positives and capacity
        assert 0 < false_positives < 100
        index_size, number_of_hashes = params_for(capacity, false_positives / 100)

    def _decor(func):
        _name = get_cache_key_template(func, key=name)
        _cache_key = f"{_name}:{index_size}"
        if prefix:
            _cache_key = f"{prefix}:{_cache_key}"

        _set = getattr(func, "set", None)

        async def __set(*args, **kwargs):
            if _set is None:
                result = await func(*args, **kwargs)
            else:
                result = await _set(*args, **kwargs)
            if not result:
                return result
            _bloom_key = get_cache_key(func, _name, args, kwargs)
            hashes = get_hashes(_bloom_key, number_of_hashes, index_size)
            await backend.incr_bits(_cache_key, *hashes)
            return result

        func.set = __set

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _bloom_key = get_cache_key(func, _name, args, kwargs)
            hashes = get_hashes(_bloom_key, number_of_hashes, index_size)
            values = await backend.get_bits(_cache_key, *hashes)
            if values is None:
                return await func(*args, **kwargs)
            if all_not_zeros(values):  # if all bits is set
                # false positive
                if check_false_positive:
                    return await func(*args, **kwargs)
                return True
            else:
                return False

        return _wrap

    return _decor


def dual_bloom(
    backend: Backend,
    name: Optional[str] = None,
    index_size: Union[int, Tuple[int, int], None] = None,
    number_of_hashes: Union[int, Tuple[int, int], None] = None,
    false: Union[float, Tuple[float, float], None] = 1,
    capacity: Union[int, Tuple[int, int], None] = None,
    no_collisions: bool = False,
    prefix: str = "dual_bloom",
):
    """
    Decorator that can help you to use bloom filter algorithm
     but this implementation with 2 bloom filters - one for true and 1 for false

    @dual_bloom(name="user_name:{name}", false_positives=1, capacity=10_000)
    async def is_user_exists(name):
        return name in ....

    :param backend: cache backend
    :param name: custom cache key
    :param index_size: size of bloom filter
    :param number_of_hashes: the same as k
    :param capacity: the same as n - number of elements
    :param no_collisions: add value only no collisions
    :param false: Percents of false results
    :param prefix: custom prefix for key, default 'dual_bloom'
    """
    filters_params = _get_params_for_filters(index_size, number_of_hashes, false, capacity)

    def _decor(func):
        _cache_key = get_cache_key_template(func, key=name)
        if prefix:
            _cache_key = f"{prefix}:{_cache_key}"
        _true_bloom_key = _cache_key + ":true"
        _false_bloom_key = _cache_key + ":false"

        __delete = getattr(func, "delete", None)

        async def _delete(*args, **kwargs):
            if __delete:
                await __delete(*args, **kwargs)
            _bloom_key = get_cache_key(func, _cache_key, args, kwargs)
            hashes_true, hashes_false = _get_hashes(_bloom_key, filters_params)
            await asyncio.gather(
                backend.incr_bits(_true_bloom_key, *hashes_true, by=-1),
                backend.incr_bits(_false_bloom_key, *hashes_false, by=-1),
            )

        func.delete = _delete

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _bloom_key = get_cache_key(func, _cache_key, args, kwargs)
            hashes_true, hashes_false = _get_hashes(_bloom_key, filters_params)
            true_values, false_values = await asyncio.gather(
                backend.get_bits(_true_bloom_key, *hashes_true),
                backend.get_bits(_false_bloom_key, *hashes_false),
            )
            if at_least_one_zero(true_values) and at_least_one_zero(false_values):
                # not set yet
                result = await func(*args, **kwargs)
                if result and (not no_collisions or all_zeros(true_values)):
                    await backend.incr_bits(_true_bloom_key, *hashes_true)
                if not result and (not no_collisions or all_zeros(false_values)):
                    await backend.incr_bits(_false_bloom_key, *hashes_false)
                return result
            if at_least_one_zero(true_values) and all(false_values):
                return False  # can be false Negative
            if at_least_one_zero(false_values) and all(true_values):
                return True  # can be false Positive
            return await func(*args, **kwargs)

        return _wrap

    return _decor


def _get_params_for_filters(
    index_size: Union[int, Tuple[int, int], None] = None,
    number_of_hashes: Union[int, Tuple[int, int], None] = None,
    false: Union[float, Tuple[float, float], None] = 1,
    capacity: Union[int, Tuple[int, int], None] = None,
):
    if index_size is None:
        assert false and capacity
        assert 0 < false < 100
        if isinstance(capacity, int):
            capacity = (capacity, capacity)
        if isinstance(false, int):
            false = (false, false)
        index_size_false, _number_of_hashes_false = params_for(capacity[0], false[0] / 100)
        index_size_true, _number_of_hashes_true = params_for(capacity[1], false[1] / 100)
        index_size = (index_size_true, index_size_false)
        number_of_hashes = (_number_of_hashes_true, _number_of_hashes_false)
    else:
        assert number_of_hashes
        if isinstance(index_size, int):
            index_size = (index_size, index_size)
        if isinstance(number_of_hashes, int):
            number_of_hashes = (number_of_hashes, number_of_hashes)
    return list(zip(index_size, number_of_hashes))


def _get_hashes(key, filters_params):
    params_true, params_false = filters_params
    index_size_true, number_of_hashes_true = params_true
    index_size_false, number_of_hashes_false = params_false
    hashes_true = get_hashes(key + "true", number_of_hashes_true, index_size_true)
    hashes_false = get_hashes(key + "false", number_of_hashes_false, index_size_false)
    return hashes_true, hashes_false


def _counting_bloom(
    backend: Backend,
    name: Optional[str] = None,
    index_size: Optional[int] = None,
    number_of_hashes: Optional[int] = None,
    false_positives: Optional[Union[float, int]] = 1,
    capacity: Optional[int] = None,
    check_false_positive: bool = True,
    prefix: str = "counting_bloom",
):
    """
    Decorator that can help you to use bloom count filter algorithm
    https://en.wikipedia.org/wiki/Counting_Bloom_filter

    :param backend: cache backend
    :param name: custom cache key
    :param index_size: size of bloom filter
    :param number_of_hashes: the same as k
    :param capacity: the same as n - number of elements
    :param false_positives: Percents of false positive results
    :param prefix: custom prefix for key, default 'counting_bloom'
    """
    if index_size is None:
        assert false_positives and capacity
        assert 0 < false_positives < 100
        index_size, number_of_hashes = params_for(capacity, false_positives / 100)

    def _decor(func):
        _name = get_cache_key_template(func, key=name)
        _cache_key = prefix + _name + f":{index_size}"

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _bloom_key = get_cache_key(func, _name, args, kwargs)
            hashes = get_hashes(_bloom_key, number_of_hashes, index_size)
            values = await backend.get_bits(_cache_key, *hashes, size=2)
            if at_least_one_zero(values):
                return False
            if all(values):
                if not check_false_positive:
                    return True
            result = await func(*args, **kwargs)
            assert isinstance(result, bool)
            if result:
                await backend.incr_bits(_cache_key, *hashes, size=2, by=3)
            else:
                await backend.incr_bits(_cache_key, *hashes, size=2, by=1)
            return result

        async def _delete(*args, **kwargs):
            _bloom_key = get_cache_key(func, _name, args, kwargs)
            hashes = get_hashes(_bloom_key, number_of_hashes, index_size)
            await backend.incr_bits(_cache_key, *hashes, size=2, by=-1)

        func.delete = _wrap.delete = _delete

        async def _set(*args, **kwargs):
            _bloom_key = get_cache_key(func, _name, args, kwargs)
            hashes = get_hashes(_bloom_key, number_of_hashes, index_size)
            await backend.incr_bits(_cache_key, *hashes, size=2, by=1)

        func.set = _wrap.set = _set

        return _wrap

    return _decor


def params_for(size: int, false_positives: float = 0.01):
    """
    size = n
    k=(m * ln 2 / n)
    m = k * n / ln 2
    p = (1-e^{-k * n/m})^{k}
    return m - number of bits in the array and k the number of hash functions
    """
    m = _count_m(size, false_positives)
    k = _count_k(m, size)
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
