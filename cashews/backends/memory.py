import asyncio
from typing import Any, Optional, Union

from .interface import Backend

__all__ = ("Memory",)


class Memory(Backend):
    def __init__(self):
        self.store = {}
        self._handlers = {}
        self._lock = asyncio.Lock()
        super().__init__()

    async def clear(self):
        self.store = {}

    async def set(self, key: str, value: Any, expire: Union[None, float, int] = None, exist=None) -> bool:
        if exist is not None:
            if not (key in self.store) is exist:
                return False
        async with self._lock:
            self._set(key, value, expire)
        return True

    async def get(self, key: str) -> Any:
        return self._get(key)

    async def incr(self, key: str):
        value = int(self._get(key) or 0) + 1
        self._set(key=key, value=value)
        return value

    async def delete(self, key: str):
        self._delete(key)

    def _delete(self, key: str) -> bool:
        if key in self.store:
            del self.store[key]
            return True
        return False

    async def expire(self, key: str, timeout: float):
        value = self._get(key)
        if value is None:
            return
        self._set(key=key, value=value, expire=timeout)

    async def ping(self, message: Optional[str] = None):
        return b"PONG" if message is None else message

    def _set(self, key: str, value: Any, expire: Optional[float] = None) -> None:
        self.store[key] = value

        if expire is not None and expire > 0.0:
            loop = asyncio.get_event_loop()
            handler = self._handlers.get(key)
            if handler:
                handler.cancel()
            self._handlers[key] = loop.call_later(expire, self._delete, key)

    def _get(self, key: str) -> Optional[Any]:
        return self.store.get(key, None)

    async def set_lock(self, key: str, value, expire):
        return await self.set(key, value, expire=expire, exist=False)

    async def is_locked(self, key: str, wait=None) -> bool:
        if wait is None:
            return key in self.store
        step = 0.001
        while wait > 0:
            if key not in self.store:
                return False
            wait -= step
            await asyncio.sleep(step)
        return key in self.store

    async def unlock(self, key, value) -> bool:
        return self._delete(key)
