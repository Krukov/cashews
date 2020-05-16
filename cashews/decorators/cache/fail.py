from functools import wraps
from typing import Optional, Tuple, Type, Union

from ...backends.interface import Backend
from ...key import get_cache_key, get_cache_key_template, register_template
from ...typing import CacheCondition
from .defaults import CacheDetect, _empty, _get_cache_condition, context_cache_detect

__all__ = ("fail",)


def fail(
    backend: Backend,
    ttl: int,
    exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
    key: Optional[str] = None,
    condition: CacheCondition = None,
    prefix: str = "fail",
):
    """
    Return cache result (at list 1 call of function call should be succeed) if call raised one of given exception,
    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param condition: callable object that determines whether the result will be saved or not
    :param exceptions: exceptions at which returned cache result
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param prefix: custom prefix for key, default "fail"
    """
    condition = _get_cache_condition(condition)

    def _decor(func):
        _key_template = f"{get_cache_key_template(func, key=key, prefix=prefix)}:{ttl}"
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, _from_cache: CacheDetect = context_cache_detect, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            try:
                result = await func(*args, **kwargs)
            except exceptions as exc:
                cached = await backend.get(_cache_key, default=_empty)
                if cached is not _empty:
                    _from_cache.set(_cache_key, ttl=ttl, exc=exc)
                    return cached
                raise exc
            else:
                if condition(result, args, kwargs):
                    await backend.set(_cache_key, result, expire=ttl)
                return result

        return _wrap

    return _decor
