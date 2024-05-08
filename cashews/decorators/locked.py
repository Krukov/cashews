from __future__ import annotations

import asyncio
import inspect
from contextvars import ContextVar
from functools import partial, wraps
from typing import TYPE_CHECKING, Callable

from cashews.backends.interface import _BackendInterface
from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

if TYPE_CHECKING:  # pragma: no cover
    from cashews._typing import TTL, DecoratedFunc, Key, KeyOrTemplate

__all__ = ("locked",)


def locked(
    backend: _BackendInterface,
    key: KeyOrTemplate | None = None,
    ttl: TTL | None = None,
    wait: bool = True,
    prefix: str = "lock",
) -> Callable[[DecoratedFunc], DecoratedFunc]:
    """
    Decorator that can help you to solve Cache stampede problem (https://en.wikipedia.org/wiki/Cache_stampede),
    Lock following function calls till first one will be finished
    Can guarantee that one function call for given ttl, if ttl is None

    :param backend: cache backend
    :param key: custom cache key, may contain alias to args or kwargs passed to a call
    :param ttl: duration to lock wrapped function call
    :param wait: if true - wait till lock is released
    :param prefix: custom prefix for key, default 'lock'
    """
    ttl = ttl_to_seconds(ttl)

    def _decor(func):
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)
        if inspect.isasyncgenfunction(func):
            return _asyncgen_lock(func, backend, ttl, _key_template, wait)
        return _coroutine_lock(func, backend, ttl, _key_template, wait)

    return _decor


def _coroutine_lock(
    func: DecoratedFunc,
    backend: _BackendInterface,
    ttl: TTL | None,
    key_template: KeyOrTemplate,
    wait: bool,
) -> DecoratedFunc:
    @wraps(func)
    async def _wrap(*args, **kwargs):
        _ttl = ttl_to_seconds(ttl, *args, **kwargs, with_callable=True)
        _cache_key = get_cache_key(func, key_template, args, kwargs)
        async with backend.lock(_cache_key, _ttl, wait=wait):
            return await func(*args, **kwargs)

    return _wrap  # type: ignore[return-value]


def _asyncgen_lock(
    func,
    backend: _BackendInterface,
    ttl: TTL | None,
    key_template: KeyOrTemplate,
    wait: bool,
):
    @wraps(func)
    async def _wrap(*args, **kwargs):
        _ttl = ttl_to_seconds(ttl, *args, **kwargs, with_callable=True)
        _cache_key = get_cache_key(func, key_template, args, kwargs)
        async with backend.lock(_cache_key, _ttl, wait=wait):
            async for chunk in func(*args, **kwargs):
                yield chunk
            return

    return _wrap


# store tasks per context to avoid awaiting another loop's task
_thunder_protection_tasks: ContextVar[dict[str, asyncio.Task]] = ContextVar("_thunder_protection_tasks", default={})


def thunder_protection(
    key: KeyOrTemplate | None = None,
) -> Callable[[DecoratedFunc], DecoratedFunc]:
    def _decor(func: DecoratedFunc) -> DecoratedFunc:
        _key_template = get_cache_key_template(func, key=key)

        def done_callback(_key: Key, _: asyncio.Task):
            _thunder_protection_tasks.set(
                {key: task for key, task in _thunder_protection_tasks.get().items() if key != _key}
            )

        @wraps(func)
        async def _wrapper(*args, **kwargs):
            _key = get_cache_key(func, _key_template, args, kwargs)
            if (task := _thunder_protection_tasks.get().get(_key)) is None:
                task = asyncio.create_task(func(*args, **kwargs))
                _thunder_protection_tasks.set({**_thunder_protection_tasks.get(), _key: task})
                task.add_done_callback(partial(done_callback, _key))

            return await task

        return _wrapper  # type: ignore[return-value]

    return _decor
