from functools import wraps
from typing import TYPE_CHECKING, Optional

from cashews._typing import TTL, AsyncCallable_T, CallableCacheCondition, Decorator, KeyOrTemplate, Tags
from cashews.formatter import register_template
from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

from ._exception import RaiseException, return_or_raise
from .defaults import _empty, context_cache_detect

if TYPE_CHECKING:  # pragma: no cover
    from cashews import Cache

__all__ = ("cache",)


def cache(
    backend: "Cache",
    ttl: TTL,
    key: Optional[KeyOrTemplate] = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "",
    tags: Tags = (),
) -> Decorator:
    """
    Simple cache strategy - trying to return cached result,
    execute wrapped call and store a result with ttl if condition return true
    :param backend: cache backend
    :param ttl: duration in seconds to store a result or a callable
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param condition: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key
    :param tags: aliases for keys that used for cache (used for invalidation)
    """

    ttl = ttl_to_seconds(ttl)

    def _decor(func: AsyncCallable_T) -> AsyncCallable_T:
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)
        register_template(func, _key_template)
        for tag in tags:
            backend.register_tag(tag, _key_template)

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _tags = [get_cache_key(func, tag, args, kwargs) for tag in tags]
            _cache_key = get_cache_key(func, _key_template, args, kwargs)

            cached = await backend.get(_cache_key, default=_empty)
            if cached is not _empty:
                context_cache_detect._set(_cache_key, ttl=ttl, name="simple", template=_key_template)
                return return_or_raise(cached)
            _exc = None
            try:
                result = await func(*args, **kwargs)
            except Exception as exc:
                _exc = exc
                result = exc

            _ttl = ttl_to_seconds(ttl, *args, **kwargs, result=result, with_callable=True)
            cond_result = condition(result, args, kwargs, key=_cache_key)
            if isinstance(cond_result, bool) and cond_result and not isinstance(result, Exception):
                await backend.set(_cache_key, result, expire=_ttl, tags=_tags)
            elif isinstance(cond_result, Exception):
                await backend.set(_cache_key, RaiseException(result), expire=_ttl, tags=_tags)
            if _exc:
                raise _exc
            return result

        return _wrap

    return _decor
