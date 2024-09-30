from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Callable

from cashews.backends.interface import _BackendInterface
from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

from .defaults import _empty, context_cache_detect

if TYPE_CHECKING:  # pragma: no cover
    from cashews._typing import TTL, CallableCacheCondition, DecoratedFunc, KeyOrTemplate

__all__ = ("failover", "fast_condition")


def fast_condition(getter, setter=None):
    def _fast_condition(result, args, kwargs, key=""):
        if getter(key):
            return False
        if setter:
            setter(key, result)
        return True

    return _fast_condition


def failover(
    backend: _BackendInterface,
    ttl: TTL,
    exceptions: type[Exception] | tuple[type[Exception]] = Exception,
    key: KeyOrTemplate | None = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "fail",
) -> Callable[[DecoratedFunc], DecoratedFunc]:
    """
    Return cache result (at list 1 call of function call should be succeed) if call raised one of given exception,
    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param condition: callable object that determines whether the result will be saved or not
    :param exceptions: exceptions at which returned cache result
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param prefix: custom prefix for key, default "fail"
    """

    ttl = ttl_to_seconds(ttl)

    def _decor(func: DecoratedFunc) -> DecoratedFunc:
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            try:
                result = await func(*args, **kwargs)
            except exceptions as exc:
                cached = await backend.get(_cache_key, default=_empty)
                if cached is not _empty:
                    _ttl = ttl_to_seconds(ttl, *args, **kwargs, result=cached, with_callable=True)
                    context_cache_detect._set(
                        _cache_key,
                        ttl=_ttl,
                        exc=exc,
                        name="failover",
                        template=_key_template,
                        value=cached,
                    )
                    return cached
                raise exc
            else:
                if condition(result, args, kwargs, key=_cache_key):
                    _ttl = ttl_to_seconds(ttl, *args, **kwargs, result=result, with_callable=True)
                    await backend.set(_cache_key, result, expire=_ttl)
                return result

        return _wrap  # type: ignore[return-value]

    return _decor
