import asyncio
from functools import wraps
from typing import Any, Callable, Dict, Optional, Union

from ..backends.interface import Backend
from ..key import get_cache_key, get_cache_key_template, get_call_values, get_func_params
from ..typing import FuncArgsType
from .defaults import CacheDetect, _default_store_condition, context_cache_detect

__all__ = ("cache", "invalidate")


def cache(
    backend: Backend,
    ttl: int,
    func_args: FuncArgsType = None,
    key: Optional[str] = None,
    store: Optional[Callable[[Any], bool]] = None,
    prefix: str = "",
):
    """
    Simple cache strategy - trying to return cached result,
    execute wrapped call and store a result with ttl if condition return true
    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param func_args: arguments that will be used in key
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param store: callable object that determines whether the result will be saved or not
    :param prefix: custom prefix for key
    """
    store = _default_store_condition if store is None else store

    def _decor(func):
        func._key_template = prefix + get_cache_key_template(func, func_args=func_args, key=key)

        @wraps(func)
        async def _wrap(*args, _from_cache: CacheDetect = context_cache_detect, **kwargs):
            _cache_key = prefix + get_cache_key(func, args, kwargs, func_args, key)
            cached = await backend.get(_cache_key)
            if cached:
                _from_cache.set(_cache_key, ttl=ttl)
                return cached
            result = await func(*args, **kwargs)
            if store(result):
                await backend.set(_cache_key, result, expire=ttl)
            return result

        return _wrap

    return _decor


async def invalidate_func(backend: Backend, func, kwargs: Optional[Dict] = None):
    key_template = getattr(func, "_key_template", None)
    if not key_template:
        return None
    values = {**{param: "*" for param in get_func_params(func)}, **kwargs}
    values = {k: str(v) if v is not None else "" for k, v in values.items()}
    return await backend.delete_match(key_template.format(**values).lower())


def invalidate(
    backend: Backend,
    target: Union[str, Callable],
    args_map: Optional[Dict[str, str]] = None,
    defaults: Optional[Dict[str, Any]] = None,
):
    args_map = args_map or {}
    defaults = defaults or {}

    def _decor(func):
        @wraps(func)
        async def _wrap(*args, **kwargs):
            result = await func(*args, **kwargs)
            _args = get_call_values(func, args, kwargs, func_args=None)
            _args.update(defaults)
            for source, dest in args_map.items():
                if dest in _args:
                    _args[source] = _args.pop(dest)
                if callable(dest):
                    _args[source] = dest(*args, **kwargs)
            if callable(target):
                asyncio.create_task(invalidate_func(backend, target, _args))
            else:
                asyncio.create_task(
                    backend.delete_match(target.format({k: str(v) if v is not None else "" for k, v in _args.items()}))
                )
            return result

        return _wrap

    return _decor
