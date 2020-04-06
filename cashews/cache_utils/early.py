import asyncio
import logging
import time
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Optional

from ..backends.interface import Backend
from ..key import get_cache_key, get_cache_key_template
from ..typing import TTL, FuncArgsType
from .defaults import CacheDetect, _default_store_condition, context_cache_detect

__all__ = ("early",)
logger = logging.getLogger(__name__)


def early(
    backend: Backend,
    ttl: TTL,
    func_args: FuncArgsType = None,
    key: Optional[str] = None,
    store: Optional[Callable[[Any], bool]] = None,
    prefix: str = "early",
):
    """
    Cache strategy that try to solve Cache stampede problem (https://en.wikipedia.org/wiki/Cache_stampede),
    With hot cache recalculate a result in background near expiration time
    Warning Not good at cold cache

    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param func_args: arguments that will be used in key
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param store: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key, default 'early'
    """
    store = _default_store_condition if store is None else store

    def _decor(func):
        func._key_template = prefix + get_cache_key_template(func, func_args=func_args, key=key)

        @wraps(func)
        async def _wrap(*args, _from_cache: CacheDetect = context_cache_detect, **kwargs):
            _cache_key = prefix + ":" + get_cache_key(func, args, kwargs, func_args, key)
            cached = await backend.get(_cache_key)
            if cached:
                _from_cache.set(_cache_key, ttl=ttl)
                expire_at, delta, result = cached
                if expire_at <= datetime.utcnow() and await backend.set(
                    _cache_key + ":hit", "1", expire=delta.total_seconds(), exist=False
                ):
                    logger.info("Recalculate cache for %s (exp_at %s)", _cache_key, expire_at)
                    asyncio.create_task(_get_result_for_early(backend, func, args, kwargs, _cache_key, ttl, store))
                    await asyncio.sleep(0)
                return result
            return await _get_result_for_early(backend, func, args, kwargs, _cache_key, ttl, store)

        return _wrap

    return _decor


async def _get_result_for_early(backend: Backend, func, args, kwargs, key, ttl: TTL, condition: Callable[[Any], bool]):
    start = time.perf_counter()
    result = await func(*args, **kwargs)
    if condition(result):
        ttl = ttl() if callable(ttl) else ttl
        ttl = ttl.total_seconds() if isinstance(ttl, timedelta) else ttl
        delta = timedelta(seconds=max([ttl - (time.perf_counter() - start) * 3, 0]))
        await backend.set(key, [datetime.utcnow() + delta, delta, result], expire=ttl)
    return result
