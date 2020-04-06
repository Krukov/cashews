import asyncio
import logging
import time
from collections import deque
from functools import wraps
from statistics import mean
from typing import Any, Callable, Iterable, Optional

from ..backends.interface import Backend
from ..key import get_cache_key
from ..typing import FuncArgsType
from .defaults import CacheDetect, _default_store_condition, context_cache_detect

__all__ = ("hit", "perf", "rate_limit", "RateLimitException", "PerfDegradationException")


logger = logging.getLogger(__name__)


def hit(
    backend: Backend,
    ttl: int,
    cache_hits: int,
    update_before: Optional[int] = None,
    func_args: FuncArgsType = None,
    key: Optional[str] = None,
    store: Optional[Callable[[Any], bool]] = None,
    prefix: str = "hit",
):
    """
    Cache call results and drop cache after given numbers of call 'cache_hits'
    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param cache_hits: number of cache hits till cache will dropped
    :param update_before: number of cache hits before cache will update
    :param func_args: arguments that will be used in key
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param disable: callable object that determines whether cache will use
    :param store: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key, default 'hit'
    """
    store = _default_store_condition if store is None else store

    def _decor(func):
        @wraps(func)
        async def _wrap(*args, _from_cache: CacheDetect = context_cache_detect, **kwargs):
            _cache_key = prefix + ":" + get_cache_key(func, args, kwargs, func_args, key)
            result = await backend.get(_cache_key)
            hits = await backend.incr(_cache_key + ":counter")
            if result and hits <= cache_hits:
                _from_cache.set(_cache_key, ttl=ttl, cache_hits=cache_hits)
                if update_before is not None and cache_hits - hits == update_before:
                    asyncio.create_task(_get_and_save(func, args, kwargs, backend, _cache_key, ttl, store))
                return result
            return await _get_and_save(func, args, kwargs, backend, _cache_key, ttl, store)

        return _wrap

    return _decor


async def _get_and_save(func, args, kwargs, backend, key, ttl, store):
    result = await func(*args, **kwargs)
    if store(result):
        await backend.delete(key + ":counter")
        await backend.set(key, result, expire=ttl)

    return result


def _default_perf_condition(current: float, previous: Iterable[float]) -> bool:
    return mean(previous) * 2 < current


class PerfDegradationException(Exception):
    pass


def perf(
    backend: Backend,
    ttl: int,
    func_args: FuncArgsType = None,
    key: Optional[str] = None,
    trace_size: int = 10,
    perf_condition: Optional[Callable[[float, Iterable[float]], bool]] = None,
    prefix: str = "perf",
):
    """
    Trace time execution of target and throw exception if it downgrade to given condition
    :param backend: cache backend
    :param ttl: duration in seconds to lock
    :param func_args: arguments that will be used in key
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param trace_size: the number of calls that are involved
    :param perf_condition: callable object that determines whether the result will be cached,
           default if doubled mean value of time execution less then current
    :param prefix: custom prefix for key, default 'perf'

    """
    perf_condition = _default_perf_condition if perf_condition is None else perf_condition
    call_results = deque([], maxlen=trace_size)

    def _decor(func):
        @wraps(func)
        async def _wrap(*args, **kwargs):
            _cache_key = prefix + ":" + get_cache_key(func, args, kwargs, func_args, key)
            if await backend.is_locked(_cache_key + ":lock"):
                raise PerfDegradationException()
            start = time.perf_counter()
            result = await func(*args, **kwargs)
            takes = time.perf_counter() - start
            if len(call_results) == trace_size and perf_condition(takes, call_results):
                await backend.set_lock(_cache_key + ":lock", value=takes, expire=ttl)
                call_results.clear()
            else:
                call_results.append(takes)
            return result

        return _wrap

    return _decor


class RateLimitException(Exception):
    pass


def _default_action(*args, **kwargs):
    raise RateLimitException()


def rate_limit(
    backend: Backend,
    limit: int,
    period: int,
    ttl: int = None,
    func_args: FuncArgsType = None,
    action: Optional[Callable] = None,
    prefix="rate_limit",
):  # pylint: disable=too-many-arguments
    """
    Rate limit for function call. Do not call function if rate limit is reached, and call given action

    :param backend: cache backend
    :param limit: number of calls
    :param period: Period
    :param ttl: time ban, default == period
    :param func_args: arguments that will be used in key
    :param action: call when rate limit reached, default raise RateLimitException
    :param prefix: custom prefix for key, default 'rate_limit'
    """
    action = _default_action if action is None else action

    def decorator(func):
        @wraps(func)
        async def wrapped_func(*args, **kwargs):
            _cache_key = prefix + ":" + get_cache_key(func, args, kwargs, func_args)

            requests_count = await backend.incr(key=_cache_key)  # set 1 if not exists
            if requests_count and requests_count > limit:
                if ttl and requests_count == limit + 1:
                    await backend.expire(key=_cache_key, timeout=ttl)
                logger.info("Rate limit reach for %s", _cache_key)
                action(*args, **kwargs)

            if requests_count == 1:
                await backend.expire(key=_cache_key, timeout=period)

            return await func(*args, **kwargs)

        return wrapped_func

    return decorator
