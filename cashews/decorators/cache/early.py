import asyncio
import logging
import time
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional

from ...backends.interface import Backend
from ...key import get_cache_key, get_cache_key_template, register_template
from ...typing import CacheCondition
from .defaults import CacheDetect, _empty, _get_cache_condition, context_cache_detect

__all__ = ("early",)
logger = logging.getLogger(__name__)


def early(
    backend: Backend, ttl: int, key: Optional[str] = None, condition: CacheCondition = None, prefix: str = "early",
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

    def _decor(func):
        _key_template = f"{get_cache_key_template(func, key=key, prefix=prefix)}:{ttl}"
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, _from_cache: CacheDetect = context_cache_detect, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            cached = await backend.get(_cache_key, default=_empty)
            if cached is not _empty:
                _from_cache.set(_cache_key, ttl=ttl)
                expire_at, delta, result = cached
                if expire_at <= datetime.utcnow() and await backend.set(
                    _cache_key + ":hit", "1", expire=delta.total_seconds(), exist=False
                ):
                    logger.info("Recalculate cache for %s (exp_at %s)", _cache_key, expire_at)
                    asyncio.create_task(_get_result_for_early(backend, func, args, kwargs, _cache_key, ttl, store))
                    # await asyncio.sleep(0)  # let loop switch to upadete cache
                return result
            return await _get_result_for_early(backend, func, args, kwargs, _cache_key, ttl, store)

        return _wrap

    return _decor


async def _get_result_for_early(backend: Backend, func, args, kwargs, key, ttl: int, condition):
    start = time.perf_counter()
    result = await func(*args, **kwargs)
    if condition(result, args, kwargs):
        delta = timedelta(seconds=max([ttl - (time.perf_counter() - start) * 3, 0]))
        await backend.set(key, [datetime.utcnow() + delta, delta, result], expire=ttl)
    return result
