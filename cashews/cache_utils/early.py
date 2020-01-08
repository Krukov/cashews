import asyncio
import logging
import time
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Dict, Optional, Union

from ..backends.interface import Backend
from ..key import FuncArgsType, get_cache_key, get_cache_key_template, get_call_values
from .defaults import _default_disable_condition, _default_store_condition

__all__ = ("early",)
logger = logging.getLogger(__name__)


def early(
    backend: Backend,
    ttl: Optional[Union[int, timedelta]],
    func_args: FuncArgsType = None,
    key: Optional[str] = None,
    disable: Optional[Callable[[Dict[str, Any]], bool]] = None,
    store: Optional[Callable[[Any], bool]] = None,
    prefix: str = "early",
):
    """
    Cache strategy that try to solve Cache stampede problem (https://en.wikipedia.org/wiki/Cache_stampede),
    With hot cache recalculate a result in background near expiration time
    Warning Not good at cold cache

    :param backend: cache backend
    :param ttl: seconds in int or as timedelta object to store a result
    :param func_args: arguments that will be used in key
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param disable: callable object that determines whether cache will use
    :param store: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key, default 'early'
    """
    store = _default_store_condition if store is None else store
    disable = _default_disable_condition if disable is None else disable

    def _decor(func):
        func._key_template = prefix + get_cache_key_template(func, func_args=func_args, key=key)

        @wraps(func)
        async def _wrap(*args, **kwargs):
            if disable(get_call_values(func, args, kwargs, func_args=None)):
                return await func(*args, **kwargs)

            _cache_key = prefix + ":" + get_cache_key(func, args, kwargs, func_args, key)
            cached = await backend.get(_cache_key)
            execution = _get_result_for_early(backend, func, args, kwargs, _cache_key, ttl, store)

            if cached:
                expire_at, delta, result = cached
                if expire_at <= datetime.utcnow() and await backend.set(
                    _cache_key + ":hit", "1", expire=delta.total_seconds(), exist=False
                ):
                    logger.info("Recalculate cache for %s (exp_at %s)", _cache_key, expire_at)
                    asyncio.create_task(execution)
                return result
            return await execution

        return _wrap

    return _decor


async def _get_result_for_early(backend: Backend, func, args, kwargs, key, ttl, condition: Callable[[Any], bool]):
    start = time.time()
    result = await func(*args, **kwargs)
    if condition(result):
        delta = timedelta(seconds=max([ttl - (time.time() - start) * 2, 0]))
        logging.info("Set result for key %s", key)
        asyncio.create_task(backend.set(key, [datetime.utcnow() + delta, delta, result], expire=ttl))
    return result
