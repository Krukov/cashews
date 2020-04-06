from functools import wraps
from typing import Optional, Tuple, Type, Union

from ..backends.interface import Backend
from ..key import get_cache_key
from ..typing import FuncArgsType
from .defaults import CacheDetect, context_cache_detect

__all__ = ("fail",)


def fail(
    backend: Backend,
    ttl: int,
    exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
    key: Optional[str] = None,
    func_args: FuncArgsType = None,
    prefix: str = "fail",
):
    """
    Return cache result (at list 1 call of function call should be succeed) if call raised one of given exception,
    :param backend: cache backend
    :param ttl: duration in seconds to store a result
    :param func_args: arguments that will be used in key
    :param exceptions: exceptions at which returned cache result
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param prefix: custom prefix for key, default "fail"
    """

    def _decor(func):
        @wraps(func)
        async def _wrap(*args, _from_cache: CacheDetect = context_cache_detect, **kwargs):
            _cache_key = prefix + ":" + get_cache_key(func, args, kwargs, func_args, key)
            try:
                result = await func(*args, **kwargs)
            except exceptions as exc:
                cached = await backend.get(_cache_key)
                if cached:
                    _from_cache.set(_cache_key, ttl=ttl, exc=exc)
                    return cached
                raise exc
            else:
                await backend.set(_cache_key, result, expire=ttl)
                return result

        return _wrap

    return _decor
