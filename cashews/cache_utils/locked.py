from functools import wraps
from typing import Optional, Union

from ..backends.interface import Backend, LockedException
from ..key import get_cache_key
from ..typing import FuncArgsType

__all__ = ("locked",)


def locked(
    backend: Backend,
    func_args: FuncArgsType = None,
    key: Optional[str] = None,
    ttl: Optional[int] = None,
    max_lock_ttl: int = 10,
    step: Union[float, int] = 0.1,
    prefix: str = "lock",
):
    """
    Decorator that can help you to solve Cache stampede problem (https://en.wikipedia.org/wiki/Cache_stampede),
    Lock following function calls till first one will be finished
    Can guarantee that one function call for given ttl, if ttl is None

    :param backend: cache backend
    :param func_args: arguments that will be used in key
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param ttl: duration to lock wrapped function call
    :param prefix: custom prefix for key, default 'early'
    """

    def _decor(func):
        @wraps(func)
        async def _wrap(*args, **kwargs):
            _cache_key = prefix + ":" + get_cache_key(func, args, kwargs, func_args, key) + ":lock"
            try:
                async with backend.lock(_cache_key, ttl or max_lock_ttl):
                    return await func(*args, **kwargs)
            except LockedException:
                if not await backend.is_locked(_cache_key, wait=ttl, step=step):
                    return await func(*args, **kwargs)
                raise

        return _wrap

    return _decor
