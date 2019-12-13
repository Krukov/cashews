import asyncio
from datetime import timedelta
from functools import wraps
from typing import Dict, Optional, Tuple, Union

from ..backends.interface import Backend, LockedException
from ..key import get_cache_key

__all__ = ("locked",)


def locked(
    backend: Backend,
    ttl: Union[int, timedelta],
    func_args: Optional[Union[Dict, Tuple]] = None,
    key: Optional[str] = None,
    lock_ttl: Union[int, timedelta] = 1,
    prefix: str = "lock",
):
    """
    Cache strategy that try to solve Cache stampede problem (https://en.wikipedia.org/wiki/Cache_stampede),
    Lock following function calls till it be cached
    Can guarantee one function call for given ttl

    :param backend: cache backend
    :param ttl: seconds in int or timedelta object to store a result
    :param func_args: arguments that will be used in key
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param lock_ttl: seconds in int or timedelta object to lock wrapped function call
            (should be more than function execution time)
    :param prefix: custom prefix for key, default 'early'
    """

    def _decor(func):
        @wraps(func)
        async def _wrap(*args, **kwargs):
            _cache_key = prefix + ":" + get_cache_key(func, args, kwargs, func_args, key)
            cached = await backend.get(_cache_key)
            if cached:
                return cached
            try:
                async with backend.lock(_cache_key + ":lock", lock_ttl):
                    result = await func(*args, **kwargs)
                    asyncio.create_task(backend.set(_cache_key, result, expire=ttl))
            except LockedException:
                await backend.is_locked(_cache_key + ":lock", wait=lock_ttl)
                result = await backend.get(_cache_key)
            return result

        return _wrap

    return _decor
