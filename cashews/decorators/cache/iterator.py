from __future__ import annotations

import time
from functools import wraps
from typing import TYPE_CHECKING, Callable

from cashews.backends.interface import _BackendInterface
from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

from ._exception import RaiseException, return_or_raise
from .defaults import context_cache_detect

if TYPE_CHECKING:  # pragma: no cover
    from cashews._typing import TTL, CallableCacheCondition, DecoratedFunc

__all__ = ("iterator",)


if "anext" not in globals():

    async def anext(ait):
        return await ait.__anext__()


def iterator(
    backend: _BackendInterface,
    ttl: TTL,
    key: str | None = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
) -> Callable[[DecoratedFunc], DecoratedFunc]:
    """
    Cache decorator for iterators
    execute wrapped call and store a result with ttl if condition return true
    :param backend: cache backend
    :param ttl: duration in seconds to store a result or a callable
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param condition: callable object that determines whether the result will be saved or not
    """

    ttl = ttl_to_seconds(ttl)

    def _decor(async_iterator: DecoratedFunc) -> DecoratedFunc:
        _key_template = get_cache_key_template(async_iterator, key=key)

        @wraps(async_iterator)
        async def _wrap(*args, **kwargs):
            _ttl = ttl_to_seconds(ttl, *args, **kwargs, with_callable=True)
            _cache_key = get_cache_key(async_iterator, _key_template, args, kwargs)

            cached = await backend.get(_cache_key)
            chunk_number = 0
            if cached:
                context_cache_detect._set(
                    _cache_key,
                    ttl=_ttl,
                    name="iterator",
                    template=_key_template,
                    value=cached,
                )
                while True:
                    chunk = await backend.get(_cache_key + f":{chunk_number}")
                    if not chunk:
                        return
                    yield return_or_raise(chunk)
                    chunk_number += 1

            start = time.monotonic()
            _to_cache = False
            _async_iterator = async_iterator(*args, **kwargs)
            while True:
                try:
                    chunk = await anext(_async_iterator)
                except StopAsyncIteration:
                    break
                except Exception as exc:
                    cond_res = condition(exc, args, kwargs, key=_cache_key)
                    if cond_res and isinstance(cond_res, Exception):
                        _to_cache = True
                        await backend.set(_cache_key + f":{chunk_number}", RaiseException(exc), expire=_ttl)
                        await backend.set(_cache_key, True, expire=_ttl - time.monotonic() + start)
                    raise exc
                yield chunk
                if condition(chunk, args, kwargs, key=_cache_key):
                    _to_cache = True
                    await backend.set(_cache_key + f":{chunk_number}", chunk, expire=_ttl)
                chunk_number += 1
            if _to_cache:
                executing_time = time.monotonic() - start
                await backend.set(_cache_key, True, expire=_ttl - executing_time)
            return

        return _wrap  # type: ignore[return-value]

    return _decor
