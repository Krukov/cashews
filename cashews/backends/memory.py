from __future__ import annotations

import asyncio
import re
import time
from collections import OrderedDict
from contextlib import suppress
from copy import copy
from typing import TYPE_CHECKING, Any, AsyncIterator, Iterable, Mapping, overload

from cashews.utils import Bitarray, get_obj_size

from .interface import NOT_EXIST, UNLIMITED, Backend

if TYPE_CHECKING:  # pragma: no cover
    from cashews._typing import Default, Key, Value


__all__ = ["Memory"]

_missed = object()


class Memory(Backend):
    """
    Inmemory backend lru with ttl
    """

    __slots__ = [
        "store",
        "_check_interval",
        "size",
        "__is_init",
        "__remove_expired_stop",
        "__remove_expired_task",
    ]

    def __init__(self, size: int = 1000, check_interval: float = 1, **kwargs):
        self.store: OrderedDict = OrderedDict()
        self._check_interval = check_interval
        self.size = size
        self.__is_init = False
        self.__remove_expired_stop = asyncio.Event()
        self.__remove_expired_task = None
        super().__init__(**kwargs)

    async def init(self):
        self.__is_init = True
        if self._check_interval:
            self.__remove_expired_stop = asyncio.Event()
            self.__remove_expired_task = asyncio.create_task(self._remove_expired())

    @property
    def is_init(self) -> bool:
        return self.__is_init

    async def _remove_expired(self):
        while not self.__remove_expired_stop.is_set():
            for key in dict(self.store):
                await self.get(key)
            with suppress(asyncio.TimeoutError, TimeoutError):
                await asyncio.wait_for(self.__remove_expired_stop.wait(), self._check_interval)

    async def clear(self):
        self.store = OrderedDict()

    async def set(
        self,
        key: Key,
        value: Value,
        expire: float | None = None,
        exist: bool | None = None,
    ) -> bool:
        if exist is not None and (key in self.store) is not exist:
            return False
        if self._serializer:
            value = await self._serializer.encode(self, key=key, value=value, expire=expire)
        self._set(key, value, expire)
        return True

    async def set_raw(self, key: Key, value: Value, **kwargs: Any) -> None:
        self.store[key] = (None, value)

    async def get(self, key: Key, default: Value | None = None) -> Value:
        return await self._get(key, default=default)

    async def get_raw(self, key: Key) -> Value:
        val = self.store.get(key)
        if val:
            return val[1]
        return None

    async def get_many(self, *keys: Key, default: Value | None = None) -> tuple[Value | None, ...]:
        values = []
        for key in keys:
            val = await self._get(key, default=default)
            if isinstance(val, Bitarray):
                continue
            values.append(val)
        return tuple(values)

    async def set_many(self, pairs: Mapping[Key, Value], expire: float | None = None):
        for key, value in pairs.items():
            if self._serializer:
                value = await self._serializer.encode(self, key=key, value=value, expire=expire)
            self._set(key, value, expire)

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Key]:
        pattern = pattern.replace("*", ".*")
        regexp = re.compile(pattern)
        for key in dict(self.store):
            if regexp.fullmatch(key):
                yield key

    async def incr(self, key: Key, value: int = 1, expire: float | None = None) -> int:
        value += int(await self._get(key, 0))
        _expire = None if value != 1 else expire
        self._set(key=key, value=value, expire=_expire)
        return value

    async def exists(self, key: Key) -> bool:
        return await self._key_exist(key)

    async def delete(self, key: Key):
        return await self._delete(key)

    async def _delete(self, key: Key) -> bool:
        if key in self.store:
            del self.store[key]
            await self._call_on_remove_callbacks(key)
            return True
        return False

    async def delete_many(self, *keys: Key):
        for key in keys:
            await self._delete(key)

    async def delete_match(self, pattern: Key):
        async for key in self.scan(pattern):
            await self._delete(key)

    async def get_match(
        self,
        pattern: str,
        batch_size: int = 100,
    ) -> AsyncIterator[tuple[Key, Value]]:
        async for key in self.scan(pattern):
            value = await self.get(key)
            if not isinstance(value, Bitarray):
                yield key, value

    async def expire(self, key: Key, timeout: float):
        if not await self._key_exist(key):
            return
        value = await self._get(key, default=_missed)
        if value is _missed:
            return
        self._set(key, value, timeout)

    async def get_expire(self, key: Key) -> int:
        if key not in self.store:
            return NOT_EXIST
        expire_at, _ = self.store[key]
        if expire_at is not None:
            return round(expire_at - time.time())
        return UNLIMITED

    async def ping(self, message: bytes | None = None) -> bytes:
        if message is None or message == b"PING":
            return b"PONG"
        return message

    async def get_bits(self, key: Key, *indexes: int, size: int = 1) -> tuple[int, ...]:
        array: Bitarray = await self._get(key, default=Bitarray("0"))
        return tuple(array.get(index, size) for index in indexes)

    async def incr_bits(self, key: Key, *indexes: int, size: int = 1, by: int = 1) -> tuple[int, ...]:
        array: Bitarray = await self._get(key, default=Bitarray("0"))
        result = []
        for index in indexes:
            array.incr(index, size, by)
            result.append(array.get(index, size))
        self._set(key, array)
        return tuple(result)

    def _set(self, key: Key, value: Value, expire: float | None = None):
        expire = time.time() + expire if expire else None
        if expire is None and key in self.store:
            expire, _ = self.store[key]
        self.store[key] = (expire, copy(value))
        self.store.move_to_end(key)
        if len(self.store) > self.size:
            self.store.popitem(last=False)

    @overload
    async def _get(self, key: Key, default: Default) -> Value | Default: ...

    @overload
    async def _get(self, key: Key, default: None = None) -> Value | None: ...

    async def _get(self, key: Key, default: Default | None = None) -> Value | None:
        if key not in self.store:
            return default
        self.store.move_to_end(key)
        expire_at, value = self.store[key]
        if expire_at and expire_at < time.time():
            await self._delete(key)
            return default
        if not self._serializer:
            return value
        return await self._serializer.decode(self, key=key, value=value, default=default)

    async def _key_exist(self, key: Key) -> bool:
        return (await self._get(key, default=_missed)) is not _missed

    async def is_locked(self, key: Key, wait: float | None = None, step: float = 0.1) -> bool:
        if wait is None:
            return await self._key_exist(key)
        while wait > 0:
            if not await self._key_exist(key):
                return False
            wait -= step
            await asyncio.sleep(step)
        return await self._key_exist(key)

    async def unlock(self, key: Key, value: Value) -> bool:
        return await self._delete(key)

    async def get_size(self, key: Key) -> int:
        if key in self.store:
            return get_obj_size(self.store[key])
        return 0

    async def slice_incr(
        self,
        key: Key,
        start: int | float,
        end: int | float,
        maxvalue: int,
        expire: float | None = None,
    ) -> int:
        val_list = await self._get(key)
        count = 0
        new_val = []
        if val_list:
            for val in val_list:
                if start <= val < end:
                    count += 1
                    new_val.append(val)
        if count < maxvalue:
            count += 1
            new_val.append(end)
        self._set(key, new_val, expire=expire)
        return count

    async def set_add(self, key: Key, *values: str, expire: float | None = None):
        val: set = await self._get(key, default=set())
        val.update(values)
        self._set(key, val, expire=expire)

    async def set_remove(self, key: Key, *values: str):
        val: set = await self._get(key, default=set())
        val.difference_update(values)
        self._set(key, val)

    async def set_pop(self, key: Key, count: int = 100) -> Iterable[str]:
        values: set = await self._get(key, default=set())
        _values = []
        for _ in range(count):
            if not values:
                break
            _values.append(values.pop())

        self._set(key, values)
        return _values

    async def get_keys_count(self) -> int:
        return len(self.store)

    async def close(self):
        if self.__remove_expired_task:
            self.__remove_expired_stop.set()
            await self.__remove_expired_task
            del self.__remove_expired_task
            self.__remove_expired_task = None
        if self.__remove_expired_stop:
            del self.__remove_expired_stop
            self.__remove_expired_stop = None
        self.__is_init = False
