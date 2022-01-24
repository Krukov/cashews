import asyncio
import logging
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional

from ..._typing import CacheCondition
from ...backends.interface import Backend
from ...formatter import register_template
from ...key import get_cache_key, get_cache_key_template
from .defaults import CacheDetect, _empty, _get_cache_condition, context_cache_detect

__all__ = ("early",)
logger = logging.getLogger(__name__)
_LOCK_SUFFIX = ":lock"


def early(
    backend: Backend,
    ttl: int,
    key: Optional[str] = None,
    early_ttl: Optional[int] = None,
    condition: CacheCondition = None,
    prefix: str = "early",
):
    """
    Cache strategy that try to solve Cache stampede problem (https://en.wikipedia.org/wiki/Cache_stampede),
    With hot cache recalculate a result in background near expiration time
    Warning Not good at cold cache

    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param condition: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key, default 'early'
    """
    store = _get_cache_condition(condition)
    if early_ttl is None:
        early_ttl = ttl * 0.33

    def _decor(func):
        _key_template = get_cache_key_template(func, key=key, prefix=prefix + ":v2")
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, _from_cache: CacheDetect = context_cache_detect, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            cached = await backend.get(_cache_key, default=_empty)
            if cached is not _empty:
                _from_cache._set(
                    _cache_key,
                    ttl=ttl,
                    early_ttl=early_ttl,
                    name="early",
                    template=_key_template,
                )
                early_expire_at, result = cached
                if early_expire_at <= datetime.utcnow() and await backend.set(
                    _cache_key + _LOCK_SUFFIX, "1", expire=early_ttl, exist=False
                ):
                    logger.info(
                        "Recalculate cache for %s (exp_at %s)",
                        _cache_key,
                        early_expire_at,
                    )
                    asyncio.create_task(
                        _get_result_for_early(
                            backend,
                            func,
                            args,
                            kwargs,
                            _cache_key,
                            ttl,
                            early_ttl,
                            store,
                        )
                    )
                return result
            return await _get_result_for_early(
                backend, func, args, kwargs, _cache_key, ttl, early_ttl, store, unlock=True
            )

        return _wrap

    return _decor


async def _get_result_for_early(
    backend: Backend, func, args, kwargs, key, ttl: int, early_ttl: int, condition, unlock=False
):
    try:
        result = await func(*args, **kwargs)
        if condition(result, args, kwargs, key):
            early_expire_at = datetime.utcnow() + timedelta(seconds=early_ttl)
            await backend.set(key, [early_expire_at, result], expire=ttl)
        return result
    finally:
        if unlock:
            asyncio.create_task(backend.delete(key + _LOCK_SUFFIX))
