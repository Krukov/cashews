from functools import wraps
from typing import Optional

from ...backends.interface import Backend
from ...key import get_cache_key, get_cache_key_template, register_template
from ...typing import CacheCondition
from .defaults import CacheDetect, _empty, _get_cache_condition, context_cache_detect

__all__ = ("cache",)


def cache(
    backend: Backend, ttl: int, key: Optional[str] = None, condition: CacheCondition = None, prefix: str = "",
):
    """
    Simple cache strategy - trying to return cached result,
    execute wrapped call and store a result with ttl if condition return true
    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param condition: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key
    """
    store = _get_cache_condition(condition)

    def _decor(func):
        _key_template = f"{get_cache_key_template(func, key=key, prefix=prefix)}:{ttl}"
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, _from_cache: CacheDetect = context_cache_detect, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            cached = await backend.get(_cache_key, default=_empty)
            if cached is not _empty:
                _from_cache.set(_cache_key, ttl=ttl)
                return cached
            result = await func(*args, **kwargs)
            if store(result, args, kwargs):
                await backend.set(_cache_key, result, expire=ttl)
            return result

        return _wrap

    return _decor
