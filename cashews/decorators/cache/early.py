from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import TYPE_CHECKING, Callable

from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

from ._exception import RaiseException, return_or_raise
from .defaults import _empty, context_cache_detect

if TYPE_CHECKING:  # pragma: no cover
    from cashews import Cache
    from cashews._typing import TTL, CallableCacheCondition, DecoratedFunc, KeyOrTemplate, Tags

__all__ = ("early",)

logger = logging.getLogger(__name__)
_LOCK_SUFFIX = ":lock"


def early(
    backend: Cache,
    ttl: TTL,
    key: KeyOrTemplate | None = None,
    early_ttl: TTL | None = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "early",
    tags: Tags = (),
    background: bool = True,
) -> Callable[[DecoratedFunc], DecoratedFunc]:
    """
    Cache strategy that try to solve Cache stampede problem (https://en.wikipedia.org/wiki/Cache_stampede),
    With hot cache recalculate a result near expiration time
    Warning Not good at cold cache

    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param early_ttl: duration in seconds to expire results
    :param condition: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key, default 'early'
    :param tags: aliases for keys that used for cache (used for invalidation)
    :param background: if true will run recalculation in background
    """

    background_tasks = set()
    ttl = ttl_to_seconds(ttl)
    early_ttl = ttl_to_seconds(early_ttl)

    def _decor(func: DecoratedFunc) -> DecoratedFunc:
        _key_template = get_cache_key_template(func, key=key, prefix=prefix + ":v2")
        for tag in tags:
            backend.register_tag(tag, _key_template)

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _ttl = ttl_to_seconds(ttl, *args, **kwargs, with_callable=True)
            if early_ttl is None:
                _early_ttl = _ttl * 0.33
            else:
                _early_ttl = ttl_to_seconds(early_ttl, *args, **kwargs, with_callable=True)
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            _tags = [get_cache_key(func, tag, args, kwargs) for tag in tags]

            args_to_call = [
                backend,
                func,
                args,
                kwargs,
                _cache_key,
                _ttl,
                _early_ttl,
                condition,
                _tags,
            ]
            cached = await backend.get(_cache_key, default=_empty)
            if cached is _empty:
                return await _get_result_for_early(*args_to_call)

            early_expire_at, result = cached
            context_cache_detect._set(
                _cache_key,
                ttl=_ttl,
                early_ttl=_early_ttl,
                name="early",
                template=_key_template,
                value=result,
                early_expire_at=early_expire_at,
            )
            if early_expire_at >= datetime.now(timezone.utc):
                return return_or_raise(result)
            lock_key = _cache_key + _LOCK_SUFFIX
            if not await backend.set(lock_key, "1", expire=_early_ttl, exist=False):
                return return_or_raise(result)
            logger.info(
                "Recalculate cache for %s (exp_at %s)",
                _cache_key,
                early_expire_at,
            )
            task = asyncio.create_task(_get_result_for_early(*args_to_call, unlock=True))
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)
            if not background:
                await task
            return return_or_raise(result)

        return _wrap  # type: ignore[return-value]

    return _decor


async def _get_result_for_early(
    backend: Cache,
    func,
    args,
    kwargs,
    key,
    ttl: int,
    early_ttl: int,
    condition,
    tags,
    unlock=False,
):
    try:
        _exc = None
        try:
            result = await func(*args, **kwargs)
        except Exception as exc:
            _exc = exc
            result = exc
        cond_result = condition(result, args, kwargs, key=key)
        early_expire_at = datetime.now(timezone.utc) + timedelta(seconds=early_ttl)
        if isinstance(cond_result, bool) and cond_result and not isinstance(result, Exception):
            await backend.set(key, [early_expire_at, result], expire=ttl, tags=tags)
        elif isinstance(cond_result, Exception):
            await backend.set(key, [early_expire_at, RaiseException(result)], expire=ttl, tags=tags)
        if _exc:
            raise _exc
        return return_or_raise(result)
    finally:
        if unlock:
            asyncio.create_task(backend.delete(key + _LOCK_SUFFIX))
