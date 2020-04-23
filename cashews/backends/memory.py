import asyncio
import datetime
import gc
import re
import sys
from typing import Any, Optional, Tuple, Union

from .interface import Backend

__all__ = ("Memory", "MemoryInterval")


class Memory(Backend):
    def __init__(self, size: int = 1000):
        self.store = {}
        self._chans = []
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

    async def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self._get(key, default=default)

    async def get_many(self, *keys: str) -> Tuple:
        return tuple([self._get(key) for key in keys])

    async def keys_match(self, pattern: str):
        pattern = pattern.replace("*", ".*")
        regexp = re.compile(pattern)
        for key in dict(self.store):
            if regexp.fullmatch(key):
                yield key

    async def incr(self, key: str):
        value = int(self._get(key) or 0) + 1
        self._set(key=key, value=value)
        return value

    async def delete(self, key: str):
        async with self._lock:
            self._delete(key)

    def _delete(self, key: str) -> bool:
        if key in self.store:
            self._notify("delete", key)
            del self.store[key]
            if key in self._meta:
                del self._meta[key]
            return True
        return False

    async def delete_match(self, pattern: str):
        async for key in self.keys_match(pattern):
            await self.delete(key)

    async def expire(self, key: str, timeout: float):
        value = self._get(key)
        if value is None:
            return
        self._set(key=key, value=value, expire=timeout)

    async def get_expire(self, key: str) -> int:
        return -1

    async def ping(self, message: Optional[bytes] = None):
        self._notify("ping", message)
        return b"PONG" if message is None else message

    def _set(self, key: str, value: Any, expire: Optional[float] = None):
        if len(self.store) > self.size:
            return
        self._notify("set", key)
        self.store[key] = value

        if expire is not None and expire > 0.0:
            loop = asyncio.get_event_loop()
            handler = self._meta.get(key)
            if handler:
                handler.cancel()
            self._meta[key] = loop.call_later(expire, self._delete, key)

    def _get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        self._notify("get", key)
        return self.store.get(key, default)

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

    async def get_size(self, key: str) -> int:
        if key in self.store:
            return _get_obj_size(self.store[key])
        return 0

    def _notify(self, cmd, key):
        for queue in self._chans:
            queue.put_nowait((cmd, key))

    async def listen(self, pattern: str, *cmds, reader=None):
        queue = asyncio.Queue(maxsize=0)
        pattern = pattern.replace("*", ".*")
        regexp = re.compile(pattern)
        self._chans.append(queue)
        try:
            while True:
                cmd, key = await queue.get()
                if cmd in cmds and regexp.fullmatch(key):
                    reader(cmd, key)
        finally:
            self._chans.remove(queue)


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

    async def get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        self._notify("get", key)
        expiration = self._meta.get(key)
        if expiration and datetime.datetime.utcnow() > expiration:
            await self.delete(key)
            return default
        return self.store.get(key, default)

    def _set(self, key: str, value: Any, expire: Optional[float] = None) -> None:
        self._notify("set", key)
        self.store[key] = value

        if expire is not None and expire > 0.0:
            self._meta[key] = datetime.datetime.utcnow() + datetime.timedelta(seconds=expire)

    async def get_expire(self, key: str) -> int:
        if key in self._meta:
            return abs(int((datetime.datetime.utcnow() - self._meta[key]).total_seconds()))
        return -1


def _get_obj_size(obj) -> int:
    marked = {id(obj)}
    obj_q = [obj]
    size = 0

    while obj_q:
        size += sum(map(sys.getsizeof, obj_q))

        # Lookup all the object referred to by the object in obj_q.
        # See: https://docs.python.org/3.7/library/gc.html#gc.get_referents
        all_refr = ((id(o), o) for o in gc.get_referents(*obj_q))

        # Filter object that are already marked.
        # Using dict notation will prevent repeated objects.
        new_refr = {o_id: o for o_id, o in all_refr if o_id not in marked and not isinstance(o, type)}

        # The new obj_q will be the ones that were not marked,
        # and we will update marked with their ids so we will
        # not traverse them again.
        obj_q = new_refr.values()
        marked.update(new_refr.keys())

    return size
