from functools import wraps
from typing import Optional, Tuple, Type, Union

from cashews._typing import TTL, AsyncCallable_T, CallableCacheCondition, Decorator, KeyOrTemplate
from cashews.backends.interface import _BackendInterface
from cashews.formatter import register_template
from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

from .defaults import _empty, context_cache_detect

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
    exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
    key: Optional[KeyOrTemplate] = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "fail",
) -> Decorator:
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

    def _decor(func: AsyncCallable_T) -> AsyncCallable_T:
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, **kwargs):
            _ttl = ttl_to_seconds(ttl, *args, **kwargs, with_callable=True)
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            try:
                result = await func(*args, **kwargs)
            except exceptions as exc:
                cached = await backend.get(_cache_key, default=_empty)
                if cached is not _empty:
                    context_cache_detect._set(
                        _cache_key,
                        ttl=_ttl,
                        exc=exc,
                        name="failover",
                        template=_key_template,
                    )
                    return cached
                raise exc
            else:
                if condition(result, args, kwargs, key=_cache_key):
                    await backend.set(_cache_key, result, expire=_ttl)
                return result

        return _wrap

    return _decor
