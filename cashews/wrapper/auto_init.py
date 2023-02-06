import asyncio

from cashews._typing import AsyncCallable_T, AsyncCallableResult_T, Middleware
from cashews.backends.interface import Backend
from cashews.commands import Command


def create_auto_init() -> Middleware:
    lock = asyncio.Lock()

    async def _auto_init(
        call: AsyncCallable_T, cmd: Command, backend: Backend, *args, **kwargs
    ) -> AsyncCallableResult_T:
        if backend.is_init:
            return await call(*args, **kwargs)
        async with lock:
            if not backend.is_init:
                await backend.init()

        return await call(*args, **kwargs)

    return _auto_init
