import asyncio
from functools import wraps
from typing import Optional

from ...backends.interface import Backend
from ...key import get_cache_key, get_cache_key_template, register_template
from ...typing import CacheCondition
from .defaults import CacheDetect, _empty, _get_cache_condition, context_cache_detect

__all__ = ("hit",)


def hit(
    backend: Backend,
    ttl: int,
    cache_hits: int,
    update_before: Optional[int] = None,
    key: Optional[str] = None,
    condition: CacheCondition = None,
    prefix: str = "hit",
):
    """
    Cache call results and drop cache after given numbers of call 'cache_hits'
    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param cache_hits: number of cache hits till cache will dropped
    :param update_before: number of cache hits before cache will update
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param condition: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key, default 'hit'
    """
    store = _get_cache_condition(condition)

    def _decor(func):
        _key_template = f"{get_cache_key_template(func, key=key, prefix=prefix)}:{ttl}"
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, _from_cache: CacheDetect = context_cache_detect, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            result = await backend.get(_cache_key, default=_empty)
            hits = await backend.incr(_cache_key + ":counter")
            if result is not _empty and hits and hits <= cache_hits:
                _from_cache.set(_cache_key, ttl=ttl, cache_hits=cache_hits)
                if update_before is not None and cache_hits - hits <= update_before:
                    asyncio.create_task(_get_and_save(func, args, kwargs, backend, _cache_key, ttl, store))
                return result
            return await _get_and_save(func, args, kwargs, backend, _cache_key, ttl, store)

        return _wrap

    return _decor


async def _get_and_save(func, args, kwargs, backend, key, ttl, store):
    result = await func(*args, **kwargs)
    if store(result, args, kwargs):
        await backend.delete(key + ":counter")
        await backend.set(key, result, expire=ttl)

    return result
