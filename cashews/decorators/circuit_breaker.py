import random
from datetime import datetime
from functools import wraps
from typing import Optional, Tuple, Type, Union

from ..backends.interface import Backend
from ..key import get_cache_key, get_cache_key_template

__all__ = ("circuit_breaker", "CircuitBreakerOpen")

_SEGMENTS = 30


class CircuitBreakerOpen(Exception):
    pass


def circuit_breaker(
    backend: Backend,
    errors_rate: int,
    period: int,
    ttl: int,
    half_open_ttl: Optional[int] = None,
    min_calls: int = 1,
    exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
    key: Optional[str] = None,
    prefix: str = "circuit_breaker",
):
    """
    Circuit breaker
    :param backend: cache backend
    :param errors_rate: Errors rate in percents
    :param period: Period
    :param ttl: seconds in int or as timedelta to keep circuit breaker switched
    :param min_calls: numbers of call before circuit breaker can switch
    :param exceptions: exceptions at which returned cache result
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param prefix: custom prefix for key, default "circuit_breaker"
    """
    assert 0 < errors_rate < 100

    def _decor(func):
        _key = ":".join([func.__module__, func.__name__])
        _key_template = get_cache_key_template(func, key=key or _key, prefix=prefix)

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            if await backend.is_locked(_cache_key + ":open"):
                if half_open_ttl:
                    await backend.set(
                        _cache_key + ":halfopen",
                        value=1,
                        expire=half_open_ttl,
                        exist=False,
                    )
                raise CircuitBreakerOpen()
            if await backend.exists(_cache_key + ":halfopen") and random.randint(0, 1):
                raise CircuitBreakerOpen()
            bucket = _get_bucket_number(period, segments=_SEGMENTS)
            in_bucket = await backend.incr(_cache_key + f":total:{bucket}") or 0
            if in_bucket == 1:
                await backend.expire(key=_cache_key + f":total:{bucket}", timeout=period)
                await backend.set(key=_cache_key + ":fails", value=0, expire=period)
            try:
                return await func(*args, **kwargs)
            except exceptions:
                fails = await backend.incr(_cache_key + ":fails")
                total = in_bucket + await _get_buckets_values(
                    backend, _cache_key, segments=_SEGMENTS, except_number=bucket
                )
                if not total < min_calls and fails * 100 / total >= errors_rate:
                    await backend.set_lock(_cache_key + ":open", value=1, expire=ttl)
                raise

        return _wrap

    return _decor


def _get_bucket_number(period: int, segments: int) -> int:
    return int((datetime.utcnow().timestamp() % period) / segments)


async def _get_buckets_values(backend: Backend, key, segments: int, except_number: int) -> int:
    keys = [f"{key}:total:{bucket}" for bucket in range(segments) if bucket != except_number]
    return sum([v for v in await backend.get_many(*keys) if v])
