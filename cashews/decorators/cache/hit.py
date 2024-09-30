from __future__ import annotations

import asyncio
from functools import wraps
from typing import TYPE_CHECKING, Callable

from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

from ._exception import RaiseException, return_or_raise
from .defaults import _empty, context_cache_detect

if TYPE_CHECKING:  # pragma: no cover
    from cashews import Cache
    from cashews._typing import TTL, CallableCacheCondition, DecoratedFunc, Key, KeyOrTemplate, Tags

__all__ = ("hit",)


def hit(
    backend: Cache,
    ttl: TTL,
    cache_hits: int,
    update_after: int | None = None,
    key: KeyOrTemplate | None = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "hit",
    tags: Tags = (),
    background: bool = True,
) -> Callable[[DecoratedFunc], DecoratedFunc]:
    """
    Cache call results and drop cache after given numbers of call 'cache_hits'
    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param cache_hits: number of cache hits till cache will dropped
    :param update_after: number of cache hits after cache will update
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param condition: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key, default 'hit'
    :param tags: aliases for keys that used for cache (used for invalidation)
    :param background: if true will run recalculation in background
    """
    ttl = ttl_to_seconds(ttl)
    background_tasks = set()

    def _decor(func: DecoratedFunc) -> DecoratedFunc:
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)
        for tag in tags:
            backend.register_tag(tag, _key_template + ":counter")
            backend.register_tag(tag, _key_template)

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            _tags = [get_cache_key(func, tag, args, kwargs) for tag in tags]

            call_args = (func, args, kwargs, backend, _cache_key, ttl, condition, _tags)

            cached, hits = await asyncio.gather(
                backend.get(_cache_key, default=_empty),
                backend.incr(_cache_key + ":counter", expire=ttl, tags=_tags),
            )
            if cached is not _empty and hits and hits <= cache_hits:
                _ttl = ttl_to_seconds(ttl, *args, **kwargs, result=cached, with_callable=True)

                context_cache_detect._set(
                    _cache_key,
                    ttl=_ttl,
                    cache_hits=cache_hits,
                    name="hit",
                    template=_key_template,
                )
                if update_after and hits == update_after:
                    task = asyncio.create_task(_get_and_save(*call_args))
                    background_tasks.add(task)
                    task.add_done_callback(background_tasks.discard)
                    if not background:
                        await task
                return return_or_raise(cached)
            return await _get_and_save(*call_args)

        return _wrap  # type: ignore[return-value]

    return _decor


async def _get_and_save(func, args, kwargs, backend: Cache, key: Key, ttl, store, tags):
    _exc = None
    try:
        result = await func(*args, **kwargs)
    except Exception as exc:
        _exc = exc
        result = exc

    ttl = ttl_to_seconds(ttl, *args, result=result, **kwargs, with_callable=True)
    cond_result = store(result, args, kwargs, key=key)
    to_cache = None
    if isinstance(cond_result, bool) and cond_result and not isinstance(result, Exception):
        to_cache = (result,)
    elif isinstance(cond_result, Exception):
        to_cache = (RaiseException(result),)

    if to_cache is not None:
        await asyncio.gather(
            backend.delete(key + ":counter"),
            backend.set(key, to_cache[0], expire=ttl, tags=tags),
        )
    if _exc:
        raise _exc
    return result
