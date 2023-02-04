from functools import wraps
from typing import Optional, Union

from cashews._typing import TTL, AsyncCallable_T, Decorator
from cashews.backends.interface import _BackendInterface
from cashews.exceptions import LockedError
from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

__all__ = ("locked",)


def locked(
    backend: _BackendInterface,
    key: Optional[str] = None,
    ttl: Optional[TTL] = None,
    max_lock_ttl: int = 10,
    step: Union[float, int] = 0.1,
    prefix: str = "lock",
) -> Decorator:
    """
    Decorator that can help you to solve Cache stampede problem (https://en.wikipedia.org/wiki/Cache_stampede),
    Lock following function calls till first one will be finished
    Can guarantee that one function call for given ttl, if ttl is None

    :param backend: cache backend
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param ttl: duration to lock wrapped function call
    :param max_lock_ttl: default ttl if it not set
    :param step: duration between lock check
    :param prefix: custom prefix for key, default 'lock'
    """
    ttl = ttl_to_seconds(ttl)

    def _decor(func: AsyncCallable_T) -> AsyncCallable_T:
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _ttl = ttl_to_seconds(ttl, *args, **kwargs, with_callable=True)
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            try:
                async with backend.lock(_cache_key, _ttl or max_lock_ttl):
                    return await func(*args, **kwargs)
            except LockedError:
                if not await backend.is_locked(_cache_key, wait=_ttl, step=step):
                    return await func(*args, **kwargs)
                raise

        return _wrap

    return _decor
