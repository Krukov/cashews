from functools import wraps
from typing import Optional

from cashews._typing import TTL, AsyncCallable_T, CallableCacheCondition, Decorator
from cashews.backends.interface import _BackendInterface
from cashews.formatter import register_template
from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

from .defaults import _empty, context_cache_detect

__all__ = ("cache",)


def cache(
    backend: _BackendInterface,
    ttl: TTL,
    key: Optional[str] = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "",
) -> Decorator:
    """
    Simple cache strategy - trying to return cached result,
    execute wrapped call and store a result with ttl if condition return true
    :param backend: cache backend
    :param ttl: duration in seconds to store a result or a callable
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param condition: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key
    """

    ttl = ttl_to_seconds(ttl)

    def _decor(func: AsyncCallable_T) -> AsyncCallable_T:
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _ttl = ttl_to_seconds(ttl, *args, **kwargs, with_callable=True)
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            cached = await backend.get(_cache_key, default=_empty)
            if cached is not _empty:
                context_cache_detect._set(_cache_key, ttl=_ttl, name="simple", template=_key_template)
                return cached
            result = await func(*args, **kwargs)
            if condition(result, args, kwargs, key=_cache_key):
                await backend.set(_cache_key, result, expire=_ttl)
            return result

        return _wrap

    return _decor
