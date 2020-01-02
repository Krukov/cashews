import asyncio
from datetime import timedelta
from functools import wraps
from typing import Any, Callable, Optional, Union

from ..backends.interface import Backend
from ..key import FuncArgsType, get_cache_key

__all__ = ("cache",)


def _default_condition(result) -> bool:
    return result is not None


def cache(
    backend: Backend,
    ttl: Union[int, timedelta],
    func_args: FuncArgsType = None,
    key: Optional[str] = None,
    condition: Optional[Callable[[Any], bool]] = None,
    prefix: str = "",
):
    """
    Simple cache strategy - trying to return cached result,
    execute wrapped call and store a result with ttl if condition return true
    :param backend: cache backend
    :param ttl: seconds in int or as timedelta object to store a result
    :param func_args: arguments that will be used in key
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param condition: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key
    """
    condition = _default_condition if condition is None else condition

    def _decor(func):
        @wraps(func)
        async def _wrap(*args, **kwargs):
            _cache_key = prefix + get_cache_key(func, args, kwargs, func_args, key)
            cached = await backend.get(_cache_key)
            if cached:
                return cached
            result = await func(*args, **kwargs)
            if condition(result):
                asyncio.create_task(backend.set(_cache_key, result, expire=ttl))
            return result

        return _wrap

    return _decor
