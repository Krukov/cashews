import logging
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, Tuple, Type, Union

from ..._typing import CacheCondition
from ...backends.interface import Backend
from ...formatter import register_template
from ...key import get_cache_key, get_cache_key_template
from .defaults import CacheDetect, _empty, _get_cache_condition, context_cache_detect

__all__ = ("soft",)
logger = logging.getLogger(__name__)


def soft(
    backend: Backend,
    ttl: int,
    key: Optional[str] = None,
    soft_ttl: Optional[int] = None,
    exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
    condition: CacheCondition = None,
    prefix: str = "soft",
):
    """
    Cache strategy that allow to use pre-expiration

    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param condition: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key, default 'early'
    """
    store = _get_cache_condition(condition)
    if soft_ttl is None:
        soft_ttl = ttl * 0.33

    def _decor(func):
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, _from_cache: CacheDetect = context_cache_detect, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            cached = await backend.get(_cache_key, default=_empty)
            if cached is not _empty:
                soft_expire_at, result = cached
                if soft_expire_at > datetime.utcnow():
                    _from_cache._set(
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
                    _from_cache._set(
                        _cache_key,
                        ttl=ttl,
                        soft_ttl=soft_ttl,
                        name="soft",
                        template=_key_template,
                    )
                    return result
                raise
            else:
                if store(result, args, kwargs, _cache_key):
                    soft_expire_at = datetime.utcnow() + timedelta(seconds=soft_ttl)
                    await backend.set(_cache_key, [soft_expire_at, result], expire=ttl)
                return result

        return _wrap

    return _decor
