import asyncio
import re
import time
from collections import OrderedDict
from typing import Any, Optional, Tuple, Union

from .interface import Backend

__all__ = "Memory"

from ..utils import _get_obj_size

_missed = object()


class Memory(Backend):
    """
    Inmemory backend lru with ttl
    """

    def __init__(self, size: int = 1000, check_interval=1):
        self.store = OrderedDict()
        self._check_interval = check_interval
        self.size = size
        self.__is_init = False
        super().__init__()

    async def init(self):
        self.__is_init = True
        asyncio.create_task(self._remove_expired())

    @property
    def is_init(self):
        return self.__is_init

    async def _remove_expired(self):
        while True:
            for key in dict(self.store):
                await self.get(key)
            await asyncio.sleep(self._check_interval)

    async def clear(self):
        self.store = OrderedDict()

    async def set(self, key: str, value: Any, expire: Union[None, float, int] = None, exist=None) -> bool:
        if exist is not None:
            if not (key in self.store) is exist:
                return False
        self._set(key, value, expire)
        return True

    async def set_row(self, key: str, value: Any, **kwargs):
        self.store[key] = value

    async def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self._get(key, default=default)

    async def get_row(self, key: str):
        return self.store.get(key)

    async def get_many(self, *keys: str) -> Tuple:
        return tuple([self._get(key) for key in keys])

    async def keys_match(self, pattern: str):
        pattern = pattern.replace("*", ".*")
        regexp = re.compile(pattern)
        for key in dict(self.store):
            if regexp.fullmatch(key):
                yield key

    async def incr(self, key: str):
        value = int(self._get(key, 0)) + 1
        self._set(key=key, value=value)
        return value

    async def exists(self, key: str):
        return self._key_exist(key)

    async def delete(self, key: str):
        return self._delete(key)

    def _delete(self, key: str) -> bool:
        if key in self.store:
            del self.store[key]
            return True
        return False

    async def delete_match(self, pattern: str):
        async for key in self.keys_match(pattern):
            self._delete(key)

    async def expire(self, key: str, timeout: float):
        if not self._key_exist(key):
            return
        value = self._get(key, default=_missed)
        if value is _missed:
            return
        self._set(key, value, timeout)

    async def get_expire(self, key: str) -> int:
        if key not in self.store:
            return -1
        expire_at, _ = self.store[key]
        return round(expire_at - time.time()) if expire_at is not None else -1

    async def ping(self, message: Optional[bytes] = None):
        return b"PONG" if message in (None, b"PING") else message

    def _set(self, key: str, value: Any, expire: Optional[float] = None):
        expire = time.time() + expire if expire else None
        if expire is None and key in self.store:
            expire, _ = self.store[key]
        self.store[key] = (expire, value)
        self.store.move_to_end(key)
        if len(self.store) > self.size:
            self.store.popitem(last=False)

    def _get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        if key not in self.store:
            return default
        self.store.move_to_end(key)
        expire_at, value = self.store.get(key)
        if expire_at and expire_at < time.time():
            self._delete(key)
            return default
        return value

    def _key_exist(self, key):
        return self._get(key, default=_missed) is not _missed

    async def set_lock(self, key: str, value, expire):
        return await self.set(key, value, expire=expire, exist=False)

    async def is_locked(self, key: str, wait=None, step=0.1) -> bool:
        if wait is None:
            return self._key_exist(key)
        while wait > 0:
            if not self._key_exist(key):
                return False
            wait -= step
            await asyncio.sleep(step)
        return self._key_exist(key)

    async def unlock(self, key, value) -> bool:
        return self._delete(key)

    async def get_size(self, key: str) -> int:
        if key in self.store:
            return _get_obj_size(self.store[key])
        return 0
