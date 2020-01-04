import asyncio
from datetime import timedelta
from functools import wraps
from typing import Any, Callable, Dict, Optional, Union

from ..backends.interface import Backend
from ..key import FuncArgsType, get_cache_key, get_cache_key_template, get_call_values

__all__ = ("cache", "invalidate")


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
        func._key_template = prefix + get_cache_key_template(func, func_args=func_args, key=key)

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


async def invalidate_func(backend: Backend, func, kwargs: Optional[Dict] = None):
    key_template = getattr(func, "_key_template", None)
    if not key_template:
        return None
    return await backend.delete(
        key_template.format(**get_call_values(func, args=(), kwargs=kwargs or {}, func_args=None))
    )


def invalidate(backend: Backend, target_func, args_map: Optional[Dict] = None):
    args_map = args_map or {}

    def _decor(func):
        @wraps(func)
        async def _wrap(*args, **kwargs):
            result = await func(*args, **kwargs)
            _args = get_call_values(func, args, kwargs, func_args=None)
            for source, dest in args_map.items():
                if dest in _args:
                    _args[source] = _args.pop(dest)
            await invalidate_func(backend, target_func, _args)
            return result

        return _wrap

    return _decor
