import logging
from datetime import datetime, timedelta
from functools import wraps
from typing import TYPE_CHECKING, Optional, Tuple, Type, Union

from cashews._typing import TTL, AsyncCallable_T, CallableCacheCondition, Decorator, KeyOrTemplate, Tags
from cashews.formatter import register_template
from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

from .defaults import _empty, context_cache_detect

if TYPE_CHECKING:  # pragma: no cover
    from cashews import Cache

__all__ = ("soft",)


logger = logging.getLogger(__name__)


def soft(
    backend: "Cache",
    ttl: TTL,
    key: Optional[KeyOrTemplate] = None,
    soft_ttl: Optional[TTL] = None,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "soft",
    tags: Tags = (),
) -> Decorator:
    """
    Cache strategy that allow to use pre-expiration

    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param exceptions: exceptions at which returned cache result if not reach ttl
    :param soft_ttl: pre expire ttl
    :param condition: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key, default 'soft'
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
                soft_expire_at, result = cached
                if soft_expire_at > datetime.utcnow():
                    context_cache_detect._set(
                        _cache_key,
                        ttl=ttl,
                        soft_ttl=soft_ttl,
                        name="soft",
                        template=_key_template,
                    )
                    return result

            try:
                result = await func(*args, **kwargs)
            except exceptions:
                if cached is not _empty:
                    _, result = cached
                    context_cache_detect._set(
                        _cache_key,
                        ttl=ttl,
                        soft_ttl=soft_ttl,
                        name="soft",
                        template=_key_template,
                    )
                    return result
                raise
            else:
                if condition(result, args, kwargs, _cache_key):
                    _ttl = ttl_to_seconds(ttl, *args, result=result, **kwargs, with_callable=True)
                    _soft_ttl = ttl_to_seconds(soft_ttl, *args, result=result, **kwargs) or _ttl * 0.33
                    soft_expire_at = datetime.utcnow() + timedelta(seconds=_soft_ttl)
                    await backend.set(_cache_key, [soft_expire_at, result], expire=_ttl, tags=_tags)
                return result

        return _wrap

    return _decor
