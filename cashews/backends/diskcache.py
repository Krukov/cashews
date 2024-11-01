from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Iterable, Mapping

from diskcache import Cache, FanoutCache

from cashews._typing import Key, Value
from cashews.serialize import DEFAULT_SERIALIZER, Serializer
from cashews.utils import Bitarray

from .interface import NOT_EXIST, UNLIMITED, Backend


class DiskCache(Backend):
    def __init__(self, *args, directory=None, shards=8, **kwargs: Any) -> None:
        serializer = kwargs.pop("serializer", DEFAULT_SERIALIZER)
        self.__is_init = False
        self._set_locks: dict[str, asyncio.Lock] = {}
        self._sharded = shards > 1
        if not self._sharded:
            self._cache = Cache(directory=directory, **kwargs)
        else:
            self._cache = FanoutCache(directory=directory, shards=shards, **kwargs)
        super().__init__(serializer=serializer, **kwargs)
        self._serializer: Serializer

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
        expire: float | None = None,
        exist: bool | None = None,
    ) -> bool:
        value = await self._serializer.encode(self, key=key, value=value, expire=expire)
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

    def _set(self, key: Key, value: Value, expire=None, exist=None):
        if exist is not None and self._exists(key) is not exist:
            return False
        if expire is None:
            expire = self._get_expire(key)
            expire = expire if expire not in [UNLIMITED, NOT_EXIST] else None
        return self._cache.set(key, value, expire)

    async def set_raw(self, key: Key, value: Any, **kwargs: Any):
        return self._cache.set(key, value, **kwargs)

    async def get(self, key: Key, default: Value | None = None) -> Value:
        value = await self._run_in_executor(self._cache.get, key, default)
        return await self._serializer.decode(self, key=key, value=value, default=default)

    async def get_raw(self, key: Key) -> Value:
        return self._cache.get(key)

    async def get_many(self, *keys: Key, default: Value | None = None) -> tuple[Value | None, ...]:
        if not keys:
            return ()
        values = await self._run_in_executor(self._get_many, keys, default)
        values = await asyncio.gather(
            *[self._serializer.decode(self, key=key, value=value, default=default) for key, value in zip(keys, values)]
        )
        return tuple(None if isinstance(value, Bitarray) else value for value in values)

    def _get_many(self, keys: list[Key], default: Value | None = None):
        values = []
        for key in keys:
            val = self._cache.get(key, default=default)
            values.append(val)
        return values

    async def set_many(self, pairs: Mapping[Key, Value], expire: float | None = None):
        _pairs = {}
        for key, value in pairs.items():
            value = await self._serializer.encode(self, key=key, value=value, expire=expire)
            _pairs[key] = value
        return await self._run_in_executor(self._set_many, _pairs, expire)

    def _set_many(self, pairs: Mapping[Key, Value], expire: float | None = None):
        for key, value in pairs.items():
            self._set(key, value, expire=expire)

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

    async def get_bits(self, key: Key, *indexes: int, size: int = 1) -> tuple[int, ...]:
        array = await self.get(key, default=Bitarray("0"))
        result = []
        for index in indexes:
            result.append(array.get(index, size))
        return tuple(result)

    async def incr_bits(self, key: str, *indexes: int, size: int = 1, by: int = 1) -> tuple[int, ...]:
        array = await self.get(key, default=Bitarray("0"))
        result = []
        for index in indexes:
            array.incr(index, size, by)
            result.append(array.get(index, size))
        await self.set(key, array)
        return tuple(result)

    async def incr(self, key: Key, value: int = 1, expire: float | None = None) -> int:
        return await self._run_in_executor(self._incr, key, value, expire)

    def _incr(self, key: Key, value: int = 1, expire: float | None = None) -> int:
        res = self._cache.incr(key, delta=value, retry=True)
        if res == 1 and expire:
            self._cache.touch(key, expire)
        return res

    async def delete(self, key: Key) -> bool:
        try:
            return await self._run_in_executor(self._cache.delete, key)
        finally:
            await self._call_on_remove_callbacks(key)

    async def delete_many(self, *keys: Key):
        try:
            await self._run_in_executor(self._delete_many, keys)
        finally:
            await self._call_on_remove_callbacks(*keys)

    def _delete_many(self, keys: list[Key]):
        for key in keys:
            self._cache.delete(key)

    async def delete_match(self, pattern: str):
        keys = []
        async for key in self.scan(pattern):
            keys.append(key)
        try:
            return await self._run_in_executor(self._delete_match, pattern)
        finally:
            await self._call_on_remove_callbacks(*keys)

    def _delete_match(self, pattern: str):
        for key in self._scan(pattern):
            self._cache.delete(key)

    async def get_match(self, pattern: str, batch_size: int = 100) -> AsyncIterator[tuple[Key, Value]]:
        if self._sharded:
            return
        for key in await self._run_in_executor(self._scan, pattern):
            value = await self.get(key)
            if isinstance(value, Bitarray):
                continue
            yield key, value

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
        return round((datetime.fromtimestamp(expire, timezone.utc) - datetime.now(timezone.utc)).total_seconds())

    async def get_size(self, key: Key) -> int:
        return -1

    async def ping(self, message: bytes | None = None) -> bytes:
        if message is None or message == b"PING":
            return b"PONG"
        return message

    async def clear(self):
        await self._run_in_executor(self._cache.clear)

    async def is_locked(
        self,
        key: Key,
        wait: float | None = None,
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
        value = await self._serializer.encode(self, key=key, value=value, expire=None)
        return await self._run_in_executor(self._unlock, key, value)

    def _unlock(self, key: Key, value: Value) -> bool:
        if self._cache.get(key) == value:
            self._cache.delete(key)
            return True
        return False

    async def slice_incr(
        self,
        key: Key,
        start: int | float,
        end: int | float,
        maxvalue: int,
        expire: float | None = None,
    ) -> int:
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

    async def set_add(self, key: Key, *values: str, expire: float | None = None):
        val = await self.get(key, default=set())
        val.update(values)
        await self.set(key, val, expire=expire)

    async def set_remove(self, key: Key, *values: str):
        val = await self.get(key, default=set())
        val.difference_update(values)
        await self.set(key, val)

    async def set_pop(self, key: Key, count: int = 100) -> Iterable[str]:
        values = await self.get(key, default=set())
        _values = []
        for _ in range(count):
            if not values:
                break
            _values.append(values.pop())

        await self.set(key, values)
        return _values

    async def get_keys_count(self) -> int:
        return await self._run_in_executor(lambda: len(self._cache))
