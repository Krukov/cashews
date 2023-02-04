import asyncio
from functools import wraps
from typing import Optional

from cashews._typing import TTL, AsyncCallable_T, CallableCacheCondition, Decorator
from cashews.backends.interface import _BackendInterface
from cashews.formatter import register_template
from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

from .defaults import _empty, context_cache_detect

__all__ = ("hit",)


def hit(
    backend: _BackendInterface,
    ttl: TTL,
    cache_hits: int,
    update_after: Optional[int] = None,
    key: Optional[str] = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "hit",
) -> Decorator:
    """
    Cache call results and drop cache after given numbers of call 'cache_hits'
    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param cache_hits: number of cache hits till cache will dropped
    :param update_after: number of cache hits after cache will update
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param condition: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key, default 'hit'
    """
    ttl = ttl_to_seconds(ttl)

    def _decor(func: AsyncCallable_T) -> AsyncCallable_T:
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _ttl = ttl_to_seconds(ttl, *args, **kwargs, with_callable=True)
            _cache_key = get_cache_key(func, _key_template, args, kwargs)

            result, hits = await asyncio.gather(
                backend.get(_cache_key, default=_empty),
                backend.incr(_cache_key + ":counter"),
            )
            if hits == 1:
                asyncio.create_task(backend.expire(_cache_key + ":counter", _ttl))
            if result is not _empty and hits and hits <= cache_hits:
                context_cache_detect._set(
                    _cache_key,
                    ttl=_ttl,
                    cache_hits=cache_hits,
                    name="hit",
                    template=_key_template,
                )
                if update_after and hits == update_after:
                    asyncio.create_task(_get_and_save(func, args, kwargs, backend, _cache_key, _ttl, condition))
                return result
            return await _get_and_save(func, args, kwargs, backend, _cache_key, _ttl, condition)

        return _wrap

    return _decor


async def _get_and_save(func, args, kwargs, backend, key, ttl, store):
    result = await func(*args, **kwargs)
    if store(result, args, kwargs, key=key):
        await asyncio.gather(
            backend.delete(key + ":counter"),
            backend.set(key, result, expire=ttl),
        )

    return result
