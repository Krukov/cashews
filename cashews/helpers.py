from __future__ import annotations

from ._typing import AsyncCallable_T, Middleware, Result_T
from .backends.interface import Backend
from .commands import PATTERN_CMDS, Command
from .utils import get_obj_size


def add_prefix(prefix: str) -> Middleware:
    async def _middleware(call: AsyncCallable_T, cmd: Command, backend: Backend, *args, **kwargs) -> Result_T:
        if cmd in (Command.GET_MANY, Command.DELETE_MANY):
            return await call(*[prefix + key for key in args])
        if cmd == Command.SET_MANY:
            kwargs["pairs"] = {prefix + key: value for key, value in kwargs["pairs"].items()}
            return await call(**kwargs)

        as_key = "pattern" if cmd in PATTERN_CMDS else "key"
        key = kwargs.get(as_key)
        if key:
            kwargs[as_key] = prefix + key
            return await call(**kwargs)
        if args:
            key = args[0].lower()
            return await call(key, *args[1:], **kwargs)
        return await call(**kwargs)

    return _middleware


def all_keys_lower() -> Middleware:
    async def _middleware(call: AsyncCallable_T, cmd: Command, backend: Backend, *args, **kwargs) -> Result_T:
        if cmd in (Command.GET_MANY, Command.DELETE_MANY):
            return await call(*[key.lower() for key in args])

        if cmd == Command.SET_MANY:
            kwargs["pairs"] = {key.lower(): value for key, value in kwargs["pairs"].items()}
            return await call(**kwargs)

        as_key = "pattern" if cmd in PATTERN_CMDS else "key"

        key = kwargs.get(as_key)
        if key:
            kwargs[as_key] = key.lower()
            return await call(**kwargs)
        if args:
            key = args[0].lower()
            return await call(key, *args[1:], **kwargs)
        return await call(**kwargs)

    return _middleware


def memory_limit(min_bytes: int = 0, max_bytes: int | None = None) -> Middleware:
    async def _middleware(call: AsyncCallable_T, cmd: Command, backend: Backend, *args, **kwargs) -> Result_T | None:
        if cmd != Command.SET:
            return await call(*args, **kwargs)
        value_size = get_obj_size(kwargs["value"])
        if max_bytes and value_size > max_bytes or value_size < min_bytes:
            return None
        return await call(*args, **kwargs)

    return _middleware
