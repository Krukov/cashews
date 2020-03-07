import asyncio
from datetime import timedelta
from functools import wraps
from typing import Optional, Tuple, Type, Union

from ..backends.interface import Backend
from ..key import FuncArgsType, get_cache_key

__all__ = ("fail", "circuit_breaker", "CircuitBreakerSwitch")


def fail(
    backend: Backend,
    ttl: Union[int, timedelta],
    exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
    key: Optional[str] = None,
    func_args: FuncArgsType = None,
    prefix: str = "fail",
):
    """
    Return cache result (at list 1 call of function call should be succeed) if call raised one of given exception,
    :param backend: cache backend
    :param ttl: seconds in int or as timedelta object to store a result
    :param func_args: arguments that will be used in key
    :param exceptions: exceptions at which returned cache result
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param prefix: custom prefix for key, default "fail"
    """

    def _decor(func):
        @wraps(func)
        async def _wrap(*args, **kwargs):
            _cache_key = prefix + ":" + get_cache_key(func, args, kwargs, func_args, key)
            try:
                result = await func(*args, **kwargs)
            except exceptions as exc:
                cached = await backend.get(_cache_key)
                if cached:
                    return cached
                raise exc
            else:
                asyncio.create_task(backend.set(_cache_key, result, expire=ttl))
                return result

        return _wrap

    return _decor


class CircuitBreakerSwitch(Exception):
    pass


def circuit_breaker(
    backend: Backend,
    ttl: Union[int, timedelta],
    limit: int,
    period: Union[int, timedelta],
    cache_ttl: Union[int, timedelta],
    exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
    key: Optional[str] = None,
    func_args: FuncArgsType = None,
    prefix: str = "circuit_breaker",
):
    """
    Circuit breaker
    :param backend: cache backend
    :param ttl: seconds in int or as timedelta to breaker work
    :param limit: number of errors
    :param period: Period
    :param cache_ttl: seconds in int or as timedelta to store result
    :param func_args: arguments that will be used in key
    :param exceptions: exceptions at which returned cache result
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param prefix: custom prefix for key, default "circuit_breaker"
    """

    def _decor(func):
        @wraps(func)
        async def _wrap(*args, **kwargs):
            _cache_key = prefix + ":" + get_cache_key(func, args, kwargs, func_args, key)
            switch = await backend.get(_cache_key + ":switch")
            if switch:
                cached = await backend.get(_cache_key)
                if cached:
                    return cached
                raise CircuitBreakerSwitch()

            try:
                result = await func(*args, **kwargs)
            except exceptions as exc:
                fails = await backend.incr(_cache_key + ":fails")
                if fails == 1:
                    await backend.expire(key=_cache_key + ":fails", timeout=period)
                if fails >= limit:
                    await backend.set(_cache_key + ":switch", True, expire=ttl)
                raise exc
            else:
                asyncio.create_task(backend.set(_cache_key, result, expire=cache_ttl))
                return result

        return _wrap

    return _decor
