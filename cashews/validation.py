from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from typing import Any, Iterator

from ._typing import AsyncCallable_T
from .backends.interface import _BackendInterface
from .commands import RETRIEVE_CMDS, Command
from .formatter import default_format
from .key import get_call_values
from .key_context import context as template_context


def invalidate(
    backend: _BackendInterface,
    key_template: str,
    args_map: dict[str, str] | None = None,
    defaults: dict[str, Any] | None = None,
):
    args_map = args_map or {}
    defaults = defaults or {}

    def _decor(func: AsyncCallable_T) -> AsyncCallable_T:
        @wraps(func)
        async def _wrap(*args, **kwargs):
            result = await func(*args, **kwargs)
            _args = get_call_values(func, args, kwargs)
            _args.update(defaults)
            for source, dest in args_map.items():
                if dest in _args:
                    _args[source] = _args.pop(dest)
            key = default_format(key_template, **_args)
            with template_context(**_args, rewrite=True):
                await backend.delete_match(key)
            return result

        return _wrap

    return _decor


_INVALIDATE_FURTHER = ContextVar("invalidate", default=False)


@contextmanager
def invalidate_further() -> Iterator[None]:
    _INVALIDATE_FURTHER.set(True)
    try:
        yield
    finally:
        _INVALIDATE_FURTHER.set(False)


async def _aiter(num=0):  # pragma: no cover
    """A trick for typing"""
    for i in range(num):
        yield i


async def _invalidate_middleware(call, cmd: Command, backend: _BackendInterface, *args, **kwargs):
    if _INVALIDATE_FURTHER.get() and cmd in RETRIEVE_CMDS:
        if "key" in kwargs:
            await backend.delete(kwargs["key"])
            return kwargs.get("default")
        if cmd == Command.GET_MATCH:
            await backend.delete_match(kwargs["pattern"])
            return _aiter()
        if cmd == Command.GET_MANY:
            await backend.delete_many(*args)
            return ()
    return await call(*args, **kwargs)
