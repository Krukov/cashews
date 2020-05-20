import asyncio
import datetime
import gc
import re
import sys
from collections import defaultdict
from typing import Any, Optional, Tuple, Union

from ..key import get_template_and_func_for
from .interface import Backend

__all__ = ("Memory", "MemoryInterval")


class Memory(Backend):
    def __init__(self, size: int = 1000, count_stat=False):
        self.store = {}
        self.size = size
        self._count_stat = count_stat
        self._counters = defaultdict(lambda: {"hit": 0, "miss": 0, "set": 0})
        self._meta = {}
        self._lock = asyncio.Lock()
        super().__init__()

    async def clear(self):
        async with self._lock:
            self.store = {}
            for handler in self._meta.values():
                handler.cancel()
            self._meta = {}

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
        if key not in self.store:
            return False
        async with self._lock:
            self._delete(key)

    def _delete(self, key: str) -> bool:
        del self.store[key]
        if key in self._meta:
            del self._meta[key]
        return True

    async def delete_match(self, pattern: str):
        async for key in self.keys_match(pattern):
            await self.delete(key)

    async def expire(self, key: str, timeout: float):
        value = self._get(key)
        if value is None:
            return
        self._set_expire_at(key, datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout))

    def _set_expire_at(self, key, date: datetime.datetime):
        loop = asyncio.get_event_loop()
        handler = self._meta.get(key)
        if handler:
            handler.cancel()
        self._meta[key] = loop.call_later((date - datetime.datetime.utcnow()).total_seconds(), self._delete, key)

    async def get_expire(self, key: str) -> int:
        return -1

    async def ping(self, message: Optional[bytes] = None):
        return b"PONG" if message is None else message

    def _set(self, key: str, value: Any, expire: Optional[float] = None):
        if len(self.store) > self.size:
            return
        self._count_set(key)
        self.store[key] = value

        if expire is not None and expire > 0.0:
            self._set_expire_at(key, datetime.datetime.utcnow() + datetime.timedelta(seconds=expire))

    def _get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        self._count_get(key)
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

    async def get_counters(self, template):
        if not self._count_stat:
            raise Exception("Can't get counters: count stat if off")
        return self._counters.get(template)

    def _count_get(self, key):
        if not self._count_stat:
            return
        template, _ = get_template_and_func_for(key)
        if template is None:
            return

        if key in self.store:
            self._counters[template]["hit"] += 1
        else:
            self._counters[template]["miss"] += 1

    def _count_set(self, key):
        if not self._count_stat:
            return
        template, _ = get_template_and_func_for(key)
        if template is None:
            return
        self._counters[template]["set"] += 1


class MemoryInterval(Memory):
    def __init__(self, size: int = 1000, check_interval: float = 1.0, count_stat=False):
        self._check_interval = check_interval
        super().__init__(size=size, count_stat=count_stat)

    async def init(self):
        asyncio.create_task(self._remove_expired())

    async def _remove_expired(self):
        while True:
            store = dict(self.store)
            for key in store:
                await self.get(key)
            await asyncio.sleep(self._check_interval)

    async def get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        expiration = self._meta.get(key)
        if expiration and datetime.datetime.utcnow() > expiration:
            await self.delete(key)
        self._count_get(key)
        return self.store.get(key, default)

    def _set_expire_at(self, key, date: datetime.datetime):
        self._meta[key] = date

    def _set(self, key: str, value: Any, expire: Optional[float] = None) -> None:
        self._count_set(key)
        self.store[key] = value

        if expire is not None and expire > 0.0:
            self._set_expire_at(key, datetime.datetime.utcnow() + datetime.timedelta(seconds=expire))

    async def get_expire(self, key: str) -> int:
        if key in self._meta:
            return abs(float((datetime.datetime.utcnow() - self._meta[key]).total_seconds()))
        return -1

    async def clear(self):
        async with self._lock:
            self.store = {}
            self._meta = {}


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
