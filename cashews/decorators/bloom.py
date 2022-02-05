
import math
from functools import wraps
from typing import Optional

from ..backends.interface import Backend
from ..key import get_cache_key, get_cache_key_template
from cashews.utils import get_hashes

__all__ = ("bloom",)


def bloom(
    backend: Backend,
    index_size: Optional[int] = None,
    number_of_hashes: Optional[int] = None,
    false_positives: Optional[int] = 0.3,
    capacity: Optional[int] = None,
    name: Optional[str] = None,
    check_false_positive: bool = True,
    prefix: str = "bloom",
):
    """
    Decorator that can help you to use bloom filter algorithm

    @bloom(**params_for(1_000_000, 0.1), name="user_name:{name}")
    async def is_user_exists(name):
        return name in ....

    :param backend: cache backend
    :param name: custom cache key
    :param index_size: size of bloom filter
    :param number_of_hashes: the same as k
    :param prefix: custom prefix for key, default 'bloom'
    """

    def _decor(func):
        _name = get_cache_key_template(func, key=name)
        _cache_key = prefix + name + f":{index_size}"

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _bloom_key = get_cache_key(func, _name, args, kwargs)
            hashes = get_hashes(_bloom_key, number_of_hashes, index_size)
            values = await backend.get_bits(_cache_key, *hashes)
            if all(values):  # if all bits is set
                # false positive
                if check_false_positive:
                    return await func(*args, **kwargs)
                return True
            else:
                return False

        async def _set(*args, **kwargs):
            _bloom_key = get_cache_key(func, _name, args, kwargs)
            hashes = get_hashes(_bloom_key, number_of_hashes, index_size)
            result = await func(*args, **kwargs)
            if result:
                await backend.incr_bits(_cache_key, *hashes)
            return result
        _wrap.set = _set

        return _wrap

    return _decor


def bloom_count(
    backend: Backend,
    index_size: int,
    number_of_hashes: int,
    name: Optional[str] = None,
    prefix: str = "bloom_count",
):
    """
    Decorator that can help you to use bloom count filter algorithm

    :param backend: cache backend
    :param name: custom cache key
    :param index_size: size of bloom filter
    :param number_of_hashes: the same as k
    :param prefix: custom prefix for key, default 'bloom'
    """

    def _decor(func):
        _name = get_cache_key_template(func, key=name)
        _cache_key = prefix + name + f":{index_size}"

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _bloom_key = get_cache_key(func, _name, args, kwargs)
            hashes = get_hashes(_bloom_key, number_of_hashes, index_size)
            values = await backend.get_bits(_cache_key, *hashes, size=2)
            if all((v == 3 for v in values)):
                return True
            if all((v >= 2 for v in values)):
                return False
            result = await func(*args, **kwargs)
            assert isinstance(result, bool)
            if result:
                await backend.incr_bits(_cache_key, *hashes, size=2, by=3)
            else:
                await backend.incr_bits(_cache_key, *hashes, size=2, by=2)
            return result

        async def _delete(*args, **kwargs):
            _bloom_key = get_cache_key(func, _name, args, kwargs)
            hashes = get_hashes(_bloom_key, number_of_hashes, index_size)
            # we cant decrement by 1 - it can lead to false negative results
            # so we should just set it to 0 - it should lead to refill a few values
            await backend.incr_bits(_cache_key, *hashes, size=2, by=-3)

        _wrap.delete = _delete

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
    return {"index_size": m, "number_of_hashes": k}


def _count_k(m, n):
    return int(round((m / n) * math.log(2)))


def _count_k_from_p(p):
    return int(math.ceil(math.log(1.0 / p, 2)))


def _count_m(n, p=0.1):
    # k = _count_k_from_p(p)
    # return int(math.ceil(
    #         (n * abs(math.log(p))) /
    #         (k * (math.log(2) ** 2))))
    return int(math.ceil(-1 * n * math.log(p) / math.log(2) ** 2))
    # return - math.ceil(n * math.log(p) / math.pow(math.log(2), 2))


def _count_probability(n, m, k):
    ev = -(k * n / m)
    ome = 1 - math.e ** ev
    return math.pow(ome, k)
