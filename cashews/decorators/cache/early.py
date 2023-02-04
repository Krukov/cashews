import asyncio
import logging
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional

from cashews._typing import TTL, AsyncCallable_T, CallableCacheCondition, Decorator
from cashews.backends.interface import _BackendInterface
from cashews.formatter import register_template
from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

from .defaults import _empty, context_cache_detect

__all__ = ("early",)


logger = logging.getLogger(__name__)
_LOCK_SUFFIX = ":lock"


def early(
    backend: _BackendInterface,
    ttl: TTL,
    key: Optional[str] = None,
    early_ttl: Optional[TTL] = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "early",
) -> Decorator:
    """
    Cache strategy that try to solve Cache stampede problem (https://en.wikipedia.org/wiki/Cache_stampede),
    With hot cache recalculate a result in background near expiration time
    Warning Not good at cold cache

    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param early_ttl: duration in seconds to expire results
    :param condition: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key, default 'early'
    """

    ttl = ttl_to_seconds(ttl)
    early_ttl = ttl_to_seconds(early_ttl)

    def _decor(func: AsyncCallable_T) -> AsyncCallable_T:
        _key_template = get_cache_key_template(func, key=key, prefix=prefix + ":v2")
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _ttl = ttl_to_seconds(ttl, *args, **kwargs, with_callable=True)
            if early_ttl is None:
                _early_ttl = _ttl * 0.33
            else:
                _early_ttl = ttl_to_seconds(early_ttl, *args, **kwargs, with_callable=True)

            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            cached = await backend.get(_cache_key, default=_empty)
            if cached is not _empty:
                context_cache_detect._set(
                    _cache_key,
                    ttl=_ttl,
                    early_ttl=_early_ttl,
                    name="early",
                    template=_key_template,
                )
                early_expire_at, result = cached
                if early_expire_at <= datetime.utcnow() and await backend.set(
                    _cache_key + _LOCK_SUFFIX, "1", expire=_early_ttl, exist=False
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
                            _ttl,
                            _early_ttl,
                            condition,
                            unlock=True,
                        )
                    )
                return result
            return await _get_result_for_early(
                backend,
                func,
                args,
                kwargs,
                _cache_key,
                _ttl,
                _early_ttl,
                condition,
            )

        return _wrap

    return _decor


async def _get_result_for_early(
    backend: _BackendInterface, func, args, kwargs, key, ttl: int, early_ttl: int, condition, unlock=False
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
