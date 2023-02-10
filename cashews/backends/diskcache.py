import asyncio
import re
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Mapping, Optional, Tuple

from diskcache import Cache, FanoutCache

from cashews._typing import Key, Tags, Value
from cashews.utils import Bitarray

from .interface import NOT_EXIST, UNLIMITED, Backend


class DiskCache(Backend):
    def __init__(self, *args, directory=None, shards=8, **kwargs: Any) -> None:
        self.__is_init = False
        self._set_locks: Dict[str, asyncio.Lock] = {}
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

    async def close(self):
        self._cache.close()
        self.__is_init = False

    async def set(
        self,
        key: Key,
        value: Value,
        expire: Optional[float] = None,
        exist: Optional[bool] = None,
        tags: Optional[Tags] = None,
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

    def _set(self, key: Key, value: Value, expire=None, exist=None, tags: Optional[Tags] = None):
        if exist is not None:
            if not self._exists(key) is exist:
                return False
        return self._cache.set(key, value, expire)

    async def set_raw(self, key: Key, value: Any, **kwargs: Any):
        return self._cache.set(key, value, **kwargs)

    async def get(self, key: Key, default: Optional[Value] = None) -> Value:
        return await self._run_in_executor(self._cache.get, key, default)

    async def get_raw(self, key: Key) -> Value:
        return self._cache.get(key)

    async def get_many(self, *keys: Key, default: Optional[Value] = None) -> Tuple[Value]:
        return await self._run_in_executor(self._get_many, keys, default)

    def _get_many(self, keys: List[Key], default: Optional[Value] = None):
        return tuple(self._cache.get(key, default=default) for key in keys)

    async def set_many(self, pairs: Mapping[Key, Value], expire: Optional[float] = None, tags: Optional[Tags] = None):
        return await self._run_in_executor(self._set_many, pairs, expire, tags)

    def _set_many(self, pairs: Mapping[Key, Value], expire: Optional[float] = None, tags: Optional[Tags] = None):
        for key, value in pairs.items():
            self._set(key, value, expire=expire, tags=tags)

    async def exists(self, key: Key) -> bool:
        return await self._run_in_executor(self._exists, key)

    def _exists(self, key: Key) -> bool:
        return key in self._cache

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Value]:  # type: ignore
        if not self._sharded:
            for key in await self._run_in_executor(self._scan, pattern):
                yield key

    def _scan(self, pattern: str) -> Value:
        pattern = pattern.replace("*", ".*")
        regexp = re.compile(pattern)
        for key in self._cache.iterkeys():
            if regexp.fullmatch(key):
                yield key

    async def get_bits(self, key: Key, *indexes: int, size: int = 1) -> Tuple[int, ...]:
        value = await self.get(key, default=0)
        array = Bitarray(str(value))
        result = []
        for index in indexes:
            result.append(array.get(index, size))
        return tuple(result)

    async def incr_bits(self, key: str, *indexes: int, size: int = 1, by: int = 1) -> Tuple[int, ...]:
        value = await self.get(key, default=0)
        array = Bitarray(str(value))
        result = []
        for index in indexes:
            array.incr(index, size, by)
            result.append(array.get(index, size))
        await self.set(key, array.to_int())
        return tuple(result)

    async def incr(self, key: Key, value: int = 1, tags: Optional[Tags] = None) -> int:
        return await self._run_in_executor(self._incr, key, value, tags)

    def _incr(self, key: Key, value: int = 1, tags: Optional[Tags] = None) -> int:
        return self._cache.incr(key, delta=value, retry=True)

    async def delete(self, key: Key) -> bool:
        return await self._run_in_executor(self._cache.delete, key)

    async def delete_many(self, *keys: Key):
        await self._run_in_executor(self._delete_many, keys)

    def _delete_many(self, keys: List[Key]):
        for key in keys:
            self._cache.delete(key)

    async def delete_match(self, pattern: str):
        return await self._run_in_executor(self._delete_match, pattern)

    def _delete_match(self, pattern: str):
        for key in self._scan(pattern):
            self._cache.delete(key)

    async def get_match(self, pattern: str, batch_size: int = None) -> AsyncIterator[Tuple[Key, Value]]:
        if not self._sharded:
            for key in await self._run_in_executor(self._scan, pattern):
                yield key, await self._run_in_executor(self._cache.get, key)

    async def expire(self, key: Key, timeout: float) -> int:
        return await self._run_in_executor(self._cache.touch, key, timeout)

    async def get_expire(self, key: Key) -> int:
        return await self._run_in_executor(self._get_expire, key)

    def _get_expire(self, key: Key) -> int:
        value, expire = self._cache.get(key, expire_time=True)
        if value is None:
            return NOT_EXIST
        if expire is None:
            return UNLIMITED
        return round((datetime.utcfromtimestamp(expire) - datetime.utcnow()).total_seconds())

    async def get_size(self, key: Key) -> int:
        return -1

    async def ping(self, message: Optional[bytes] = None) -> bytes:
        return b"PONG" if message in (None, b"PING") else message

    async def clear(self):
        await self._run_in_executor(self._cache.clear)

    async def set_lock(self, key: Key, value: Value, expire: float) -> bool:
        return await self.set(key, value, expire=expire, exist=False)

    async def is_locked(
        self,
        key: Key,
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

    async def unlock(self, key: Key, value: Value) -> bool:
        return await self.delete(key)

    async def slice_incr(self, key: Key, start: int, end: int, maxvalue: int, expire: Optional[float] = None) -> int:
        val_set = await self.get(key)
        count = 0
        new_val = []
        if val_set:
            for val in val_set:
                if start <= val <= end:
                    count += 1
                    new_val.append(val)

        if count < maxvalue:
            count += 1
            new_val.append(end)
        await self.set(key, new_val, expire=expire)
        return count
