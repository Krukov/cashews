from __future__ import annotations

from ._typing import AsyncCallable_T, Middleware, Result_T
from .backends.interface import Backend
from .commands import Command
from .utils import get_obj_size


def memory_limit(min_bytes=0, max_bytes=None) -> Middleware:
    async def _middleware(call: AsyncCallable_T, cmd: Command, backend: Backend, *args, **kwargs) -> Result_T | None:
        if cmd == Command.SET:
            value_size = get_obj_size(kwargs["value"])
            if max_bytes and value_size > max_bytes or value_size < min_bytes:
                return None
        if cmd == Command.SET_MANY:
            pairs = {}
            for key, value in kwargs["pairs"].items():
                value_size = get_obj_size(value)
                if max_bytes and value_size > max_bytes or value_size < min_bytes:
                    continue
                pairs[key] = value
            if not pairs:
                return None
            kwargs["pairs"] = pairs
        return await call(*args, **kwargs)

    return _middleware
