import asyncio
import re
from datetime import datetime
from typing import Any, AsyncIterator, List, Optional, Tuple

from diskcache import Cache, FanoutCache

from cashews.utils import Bitarray

from .interface import Backend


class DiskCache(Backend):
    name = "diskcache"

    def __init__(self, *args, directory=None, shards=8, **kwargs):
        self.__is_init = False
        self._set_locks = {}
        self._sharded = shards > 1
        if not self._sharded:
            self._cache = Cache(directory=directory, **kwargs)
        else:
            self._cache = FanoutCache(directory=directory, shards=shards, **kwargs)
        super().__init__()

    async def init(self):
        self.__is_init = True

    async def _run_in_executor(self, call, *args):
        return await asyncio.get_running_loop().run_in_executor(None, call, *args)

    @property
    def is_init(self) -> bool:
        return self.__is_init

    def close(self):
        self._cache.close()

    async def set(
        self,
        key: str,
        value: Any,
        expire: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        future = self._run_in_executor(self._set, key, value, expire, exist)
        if exist is not None:
            # we should have async lock until value real set
            lock = self._set_locks.setdefault(key, asyncio.Lock())
            async with lock:
                try:
                    return await future
                finally:
                    self._set_locks.pop(key, None)
        return await future

    def _set(self, key, value, expire=None, exist=None):
        if exist is not None:
            if not self._exists(key) is exist:
                return False
        return self._cache.set(key, value, expire)

    async def set_raw(self, key: str, value: Any, **kwargs):
        return self._cache.set(key, value, **kwargs)

    async def get(self, key: str, default: Optional[Any] = None) -> Any:
        return await self._run_in_executor(self._cache.get, key, default)

    async def get_raw(self, key: str) -> Any:
        return self._cache.get(key)

    async def get_many(self, *keys: str, default: Optional[Any] = None) -> Tuple[Any]:
        return await self._run_in_executor(self._get_many, keys, default)

    def _get_many(self, keys: List[str], default: Optional[Any] = None):
        return tuple(self._cache.get(key, default=default) for key in keys)

    async def exists(self, key: str) -> bool:
        return await self._run_in_executor(self._exists, key)

    def _exists(self, key: str) -> bool:
        return key in self._cache

    async def keys_match(self, pattern: str):
        if not self._sharded:
            for key in await self._run_in_executor(self._keys_match, pattern):
                yield key

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:
        async for key in self.keys_match(pattern):
            yield key

    def _keys_match(self, pattern: str):
        pattern = pattern.replace("*", ".*")
        regexp = re.compile(pattern)
        for key in self._cache.iterkeys():
            if regexp.fullmatch(key):
                yield key

    async def get_bits(self, key: str, *indexes: int, size: int = 1) -> Tuple[int]:
        value = await self.get(key, default=0)
        array = Bitarray(str(value))
        result = []
        for index in indexes:
            result.append(array.get(index, size))
        return tuple(result)

    async def incr_bits(self, key: str, *indexes: int, size: int = 1, by: int = 1) -> Tuple[int]:
        value = await self.get(key, default=0)
        array = Bitarray(str(value))
        result = []
        for index in indexes:
            array.incr(index, size, by)
            result.append(array.get(index, size))
        await self.set(key, array.to_int())
        return tuple(result)

    async def incr(self, key: str) -> int:
        return await self._run_in_executor(self._cache.incr, key)

    async def delete(self, key: str) -> bool:
        return await self._run_in_executor(self._cache.delete, key)

    async def delete_match(self, pattern: str):
        return await self._run_in_executor(self._delete_match, pattern)

    def _delete_match(self, pattern: str):
        for key in self._keys_match(pattern):
            self._cache.delete(key)

    async def get_match(
        self, pattern: str, batch_size: int = None, default: Optional[Any] = None
    ) -> AsyncIterator[Tuple[str, Any]]:
        if not self._sharded:
            for key in await self._run_in_executor(self._keys_match, pattern):
                yield key, await self._run_in_executor(self._cache.get, key, default)

    async def expire(self, key: str, timeout: float) -> int:
        return await self._run_in_executor(self._cache.touch, key, timeout)

    async def get_expire(self, key: str) -> int:
        return await self._run_in_executor(self._get_expire, key)

    def _get_expire(self, key: str):
        _, expire = self._cache.get(key, expire_time=True)
        if expire is None:
            return -1
        return round((datetime.utcfromtimestamp(expire) - datetime.utcnow()).total_seconds())

    async def get_size(self, key: str) -> int:
        return -1

    async def ping(self, message: Optional[bytes] = None) -> bytes:
        return message or b"PONG"

    async def clear(self):
        await self._run_in_executor(self._cache.clear)

    async def set_lock(self, key: str, value: Any, expire: float) -> bool:
        return await self.set(key, value, expire=expire, exist=False)

    async def is_locked(
        self,
        key: str,
        wait: Optional[float] = None,
        step: float = 0.1,
    ) -> bool:
        if wait is None:
            return await self.exists(key)
        while wait > 0:
            if not await self.exists(key):
                return False
            wait -= step
            await asyncio.sleep(step)
        return await self.exists(key)

    async def unlock(self, key: str, value: Any) -> bool:
        return await self.delete(key)
