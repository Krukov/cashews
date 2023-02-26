import time
from functools import wraps
from typing import Optional

from cashews._typing import TTL, AsyncCallable_T, CallableCacheCondition, Decorator
from cashews.backends.interface import _BackendInterface
from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

from .defaults import context_cache_detect

__all__ = ("iterator",)


def iterator(
    backend: _BackendInterface,
    ttl: TTL,
    key: Optional[str] = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
) -> Decorator:
    """
    Cache decorator for iterators
    execute wrapped call and store a result with ttl if condition return true
    :param backend: cache backend
    :param ttl: duration in seconds to store a result or a callable
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param condition: callable object that determines whether the result will be saved or not
    """

    ttl = ttl_to_seconds(ttl)

    def _decor(async_iterator: AsyncCallable_T) -> AsyncCallable_T:
        _key_template = get_cache_key_template(async_iterator, key=key)

        @wraps(async_iterator)
        async def _wrap(*args, **kwargs):
            _ttl = ttl_to_seconds(ttl, *args, **kwargs, with_callable=True)
            _cache_key = get_cache_key(async_iterator, _key_template, args, kwargs)

            cached = await backend.get(_cache_key)
            chunk_number = 0
            if cached:
                context_cache_detect._set(_cache_key, ttl=_ttl, name="iterator", template=_key_template)
                while True:
                    chunk = await backend.get(_cache_key + f":{chunk_number}")
                    if not chunk:
                        return
                    yield chunk
                    chunk_number += 1

            _to_cache = condition(None, args, kwargs, key=_cache_key)
            start = time.monotonic()
            async for chunk in async_iterator(*args, **kwargs):
                yield chunk
                if _to_cache:
                    await backend.set(_cache_key + f":{chunk_number}", chunk, expire=_ttl)
                chunk_number += 1
            if _to_cache:
                executing_time = time.monotonic() - start
                await backend.set(_cache_key, True, expire=_ttl - executing_time)
            return

        return _wrap

    return _decor
