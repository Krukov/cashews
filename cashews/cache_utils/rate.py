import asyncio
import logging
import time
from collections import deque
from datetime import timedelta
from functools import wraps
from statistics import mean
from typing import Any, Callable, Dict, Iterable, Optional, Union

from ..backends.interface import Backend
from ..key import FuncArgsType, get_cache_key, get_call_values
from .defaults import _default_disable_condition, _default_store_condition

__all__ = ("hit", "perf", "rate_limit")


logger = logging.getLogger(__name__)


def hit(
    backend: Backend,
    ttl: Union[int, timedelta],
    cache_hits: int,
    update_before: int = 0,
    func_args: FuncArgsType = None,
    key: Optional[str] = None,
    disable: Optional[Callable[[Dict[str, Any]], bool]] = None,
    store: Optional[Callable[[Any], bool]] = None,
    prefix: str = "hit",
):
    """
    Cache call results and drop cache after given numbers of call 'cache_hits'
    :param backend: cache backend
    :param ttl: seconds in int or as timedelta object to store a result
    :param cache_hits: number of cache hits till cache will dropped
    :param update_before: number of cache hits before cache will update
    :param func_args: arguments that will be used in key
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param disable: callable object that determines whether cache will use
    :param store: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key, default 'hit'
    """
    store = _default_store_condition if store is None else store
    disable = _default_disable_condition if disable is None else disable

    def _decor(func):
        @wraps(func)
        async def _wrap(*args, **kwargs):
            if disable(get_call_values(func, args, kwargs, func_args=None)):
                return await func(*args, **kwargs)

            _cache_key = prefix + ":" + get_cache_key(func, args, kwargs, func_args, key)
            result = await backend.get(_cache_key)
            hits = await backend.incr(_cache_key + ":counter")
            if result and hits <= cache_hits:
                if update_before and cache_hits - hits == update_before:
                    asyncio.create_task(_get_and_save(func, args, kwargs, backend, _cache_key, ttl))
                return result
            result = await func(*args, **kwargs)
            if store(result):
                asyncio.create_task(backend.delete(_cache_key + ":counter"))
                asyncio.create_task(backend.set(_cache_key, result, expire=ttl))
            return result

        return _wrap

    return _decor


async def _get_and_save(func, args, kwargs, backend, key, ttl):
    result = await func(*args, **kwargs)
    await backend.set(key, result, expire=ttl)


def _default_perf_condition(current: float, previous: Iterable[float]) -> bool:
    return mean(previous) * 2 < current


def perf(
    backend: Backend,
    ttl: Union[int, timedelta],
    func_args: FuncArgsType = None,
    key: Optional[str] = None,
    trace_size: int = 10,
    perf_condition: Optional[Callable[[float, Iterable[float]], bool]] = None,
    prefix: str = "perf",
):
    """
    Trace time execution of target and enable cache if it downgrade to given condition
    :param backend: cache backend
    :param ttl: seconds in int or as timedelta object to store a result
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
                cached = await backend.get(_cache_key)
                if cached:
                    return cached
            start = time.time()
            result = await func(*args, **kwargs)
            takes = time.time() - start
            if len(call_results) == trace_size and perf_condition(takes, call_results):
                await backend.set_lock(_cache_key + ":lock", value=takes, expire=ttl)
                await backend.set(_cache_key, result, expire=ttl)
                return result
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
    period: Union[int, timedelta],
    ttl: Optional[Union[int, timedelta]] = None,
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
