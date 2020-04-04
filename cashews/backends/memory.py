import asyncio
import datetime
import re
from typing import Any, Optional, Tuple, Union

from .interface import Backend

__all__ = ("Memory", "MemoryInterval")


class Memory(Backend):
    def __init__(self, size: int = 1000):
        self.store = {}
        self.size = size
        self._meta = {}
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

    async def get_many(self, *keys: str) -> Tuple:
        return tuple([self._get(key) for key in keys])

    async def incr(self, key: str):
        value = int(self._get(key) or 0) + 1
        self._set(key=key, value=value)
        return value

    async def delete(self, key: str):
        async with self._lock:
            self._delete(key)

    def _delete(self, key: str) -> bool:
        if key in self.store:
            del self.store[key]
            if key in self._meta:
                del self._meta[key]
            return True
        return False

    async def delete_match(self, pattern: str):
        pattern = pattern.replace("*", "[^:]+")
        regexp = re.compile(pattern)
        for key in dict(self.store):
            if regexp.fullmatch(key):
                await self.delete(key)

    async def expire(self, key: str, timeout: float):
        value = self._get(key)
        if value is None:
            return
        self._set(key=key, value=value, expire=timeout)

    async def get_expire(self, key: str) -> int:
        return -1

    async def ping(self, message: Optional[str] = None):
        return b"PONG" if message is None else message

    def _set(self, key: str, value: Any, expire: Optional[float] = None):
        if len(self.store) > self.size:
            return
        self.store[key] = value

        if expire is not None and expire > 0.0:
            loop = asyncio.get_event_loop()
            handler = self._meta.get(key)
            if handler:
                handler.cancel()
            self._meta[key] = loop.call_later(expire, self._delete, key)

    def _get(self, key: str) -> Optional[Any]:
        return self.store.get(key, None)

    async def set_lock(self, key: str, value, expire):
        return await self.set(key, value, expire=expire, exist=False)

    async def is_locked(self, key: str, wait=None, step=0.1) -> bool:
        if wait is None:
            return key in self.store
        while wait > 0:
            if key not in self.store:
                return False
            wait -= step
            await asyncio.sleep(step)
        return key in self.store

    async def unlock(self, key, value) -> bool:
        return self._delete(key)


class MemoryInterval(Memory):
    def __init__(self, size: int = 1000, check_interval: float = 1.0):
        self._check_interval = check_interval
        super().__init__(size=size)

    async def init(self):
        asyncio.create_task(self._remove_expired())

    async def _remove_expired(self):
        while True:
            store = dict(self.store)
            for key in store:
                await self.get(key)
            await asyncio.sleep(self._check_interval)

    async def get(self, key: str) -> Optional[Any]:
        expiration = self._meta.get(key)
        if expiration and datetime.datetime.utcnow() > expiration:
            await self.delete(key)
            return None

        return self.store.get(key, None)

    def _set(self, key: str, value: Any, expire: Optional[float] = None) -> None:
        self.store[key] = value

        if expire is not None and expire > 0.0:
            self._meta[key] = datetime.datetime.utcnow() + datetime.timedelta(seconds=expire)

    async def get_expire(self, key: str) -> int:
        if key in self._meta:
            return abs(int((datetime.datetime.utcnow() - self._meta[key]).total_seconds()))
        return -1
