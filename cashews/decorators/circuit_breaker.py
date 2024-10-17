from __future__ import annotations

import random
from datetime import datetime, timezone
from functools import wraps
from typing import TYPE_CHECKING, Callable

from cashews.backends.interface import _BackendInterface
from cashews.exceptions import CircuitBreakerOpen
from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

if TYPE_CHECKING:  # pragma: no cover
    from cashews._typing import TTL, DecoratedFunc, Exceptions, Key, KeyOrTemplate


def circuit_breaker(
    backend: _BackendInterface,
    errors_rate: int,
    period: TTL,
    ttl: TTL,
    half_open_ttl: TTL | None = None,
    min_calls: int = 1,
    exceptions: Exceptions = Exception,
    key: KeyOrTemplate | None = None,
    prefix: str = "circuit_breaker",
) -> Callable[[DecoratedFunc], DecoratedFunc]:
    """
    Circuit breaker
    :param backend: cache backend
    :param errors_rate: Errors rate in percents
    :param period: Period
    :param ttl: seconds in int or as timedelta to keep circuit breaker switched
    :param min_calls: numbers of call before circuit breaker can switch
    :param exceptions: exceptions at which returned cache result
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param prefix: custom prefix for key, default "circuit_breaker"
    """
    ttl = ttl_to_seconds(ttl)
    period = ttl_to_seconds(period)
    half_open_ttl = ttl_to_seconds(half_open_ttl)
    assert 0 < errors_rate < 100

    def _decor(func: DecoratedFunc) -> DecoratedFunc:
        _key = ":".join([func.__module__, func.__name__])
        _key_template = get_cache_key_template(func, key=key or _key, prefix=prefix)

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            if await backend.is_locked(_cache_key + ":open"):
                if half_open_ttl:
                    await backend.set(
                        _cache_key + ":halfopen",
                        value=1,
                        expire=half_open_ttl,
                        exist=False,
                    )
                raise CircuitBreakerOpen()
            if await backend.exists(_cache_key + ":halfopen") and random.randint(0, 1):
                raise CircuitBreakerOpen()
            total = await _get_requests_count(backend, _cache_key + ":total", period)
            try:
                return await func(*args, **kwargs)
            except exceptions:
                fails = await _get_requests_count(backend, _cache_key + ":fails", period)
                if total and not total < min_calls and fails * 100 / total >= errors_rate:
                    await backend.set_lock(_cache_key + ":open", value=1, expire=ttl)
                raise

        return _wrap  # type: ignore[return-value]

    return _decor


async def _get_requests_count(backend: _BackendInterface, key: Key, period: int) -> int:
    timestamp = datetime.now(timezone.utc).timestamp()
    return await backend.slice_incr(key, timestamp - period, timestamp, 9999, expire=period)
