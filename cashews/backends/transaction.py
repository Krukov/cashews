import asyncio
import time
from typing import Any, AsyncIterator, Iterable, Mapping, Optional, Tuple
from uuid import uuid4

from cashews import LockedError
from cashews._typing import Callback, Key, Value
from cashews.backends.interface import NOT_EXIST, UNLIMITED, Backend
from cashews.backends.memory import Memory

_empty = object()
_GLOBAL_LOCK_KEY = ":serializable:lock"
_LOCK_PREFIX = ":tx_lock"


class TransactionBackend(Backend):
    __slots__ = [
        "_backend",
        "_local_cache",
        "_to_delete",
        "__disable",
    ]

    def __init__(self, backend: Backend):
        self._backend = backend
        self._local_cache = Memory()
        self._to_delete = set()
        super().__init__()

    def _key_is_delete(self, key: Key) -> bool:
        if key in self._to_delete:
            return True
        return False

    async def commit(self):
        if self._to_delete:
            await self._backend.delete_many(*self._to_delete)

        expire_group = {}
        for key, (expire, value) in self._local_cache.store.items():
            if expire:
                expire = int(expire - time.time())
            expire_group.setdefault(expire, {})[key] = value

        for expire, kv in expire_group.items():
            await self._backend.set_many(kv, expire=expire)
        self._clear_local_storage()

    async def rollback(self):
        self._clear_local_storage()

    def _clear_local_storage(self):
        self._local_cache = Memory()
        self._to_delete = set()

    def on_remove_callback(self, callback: Callback):
        self._backend.on_remove_callback(callback)
        self._local_cache.on_remove_callback(callback)

    async def set(
        self,
        key: Key,
        value: Value,
        expire: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        if exist is not None:
            if await self._backend.exists(key) is not exist:
                if await self._local_cache.exists(key) is not exist:
                    return False
        self._to_delete.discard(key)
        return await self._local_cache.set(key, value, expire, exist)

    async def set_many(self, pairs: Mapping[Key, Value], expire: Optional[float] = None):
        self._to_delete.difference_update(pairs.keys())
        return await self._local_cache.set_many(pairs, expire)

    async def incr(self, key: Key, value: int = 1, expire: Optional[float] = None) -> int:
        if not await self._local_cache.exists(key) and key not in self._to_delete:
            current = await self._backend.get(key, 0)
            await self._local_cache.set(key, current)
        self._to_delete.discard(key)
        return await self._local_cache.incr(key, value, expire=expire)

    async def delete(self, key: Key) -> bool:
        await self._local_cache.delete(key)
        self._to_delete.add(key)
        return True

    async def delete_many(self, *keys: Key):
        await self._local_cache.delete_many(*keys)
        self._to_delete.update(keys)

    async def delete_match(self, pattern: str):
        await self._local_cache.delete_match(pattern)
        async for key in self._backend.scan(pattern):
            self._to_delete.add(key)

    async def expire(self, key: Key, timeout: float):
        if self._key_is_delete(key):
            return
        value = await self._backend.get(key, default=_empty)
        if value is _empty:
            return await self._local_cache.expire(key, timeout)
        await self._local_cache.set(key, value, expire=timeout)

    # non transaction - proxy methods with custom logic
    async def get(self, key: str, default: Optional[Value] = None) -> Value:
        if self._key_is_delete(key):
            return default
        value = await self._local_cache.get(key, default=_empty)
        if value is not _empty:
            return value
        return await self._backend.get(key, default=default)

    async def get_many(self, *keys: Key, default: Optional[Value] = None) -> Tuple[Optional[Value], ...]:
        missed_keys = set(keys)
        values = {key: default for key in keys}

        _keys = list(missed_keys)
        for i, value in enumerate(await self._local_cache.get_many(*_keys, default=_empty)):
            if value is not _empty:
                key = _keys[i]
                values[key] = value
                missed_keys.remove(key)

        _keys = list(missed_keys)
        for i, value in enumerate(await self._backend.get_many(*_keys, default=default)):
            key = _keys[i]
            if not self._key_is_delete(key):
                values[key] = value
        return tuple(values[key] for key in keys)

    async def get_match(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Tuple[Key, Value]]:
        _local_state = set()
        async for key, value in self._local_cache.get_match(pattern):
            yield key, value
            _local_state.add(key)
        async for key, value in self._backend.get_match(pattern, batch_size=batch_size):
            if self._key_is_delete(key):
                continue
            if key in _local_state:
                continue
            yield key, value

    async def get_expire(self, key: Key) -> int:
        if self._key_is_delete(key):
            return NOT_EXIST
        local_expire = await self._local_cache.get_expire(key)
        if local_expire >= 0:
            return local_expire
        backend_expire = await self._backend.get_expire(key)
        if backend_expire is NOT_EXIST and local_expire is UNLIMITED:
            return UNLIMITED
        return backend_expire

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Key]:
        _local_state = set()
        async for key in self._local_cache.scan(pattern):
            yield key
            _local_state.add(key)
        async for key in self._backend.scan(pattern, batch_size=batch_size):
            if self._key_is_delete(key):
                continue
            if key in _local_state:
                continue
            yield key

    async def exists(self, key: Key) -> bool:
        if await self._local_cache.exists(key):
            return True
        if self._key_is_delete(key):
            return False
        return await self._backend.exists(key)

    # non transaction - proxy methods
    async def get_keys_count(self) -> int:
        return await self._backend.get_keys_count() + await self._local_cache.get_keys_count()

    @property
    def is_init(self) -> bool:
        return self._backend.is_init

    async def init(self):
        return await self._backend.init()

    async def close(self):
        return await self._backend.close()

    async def set_raw(self, key: Key, value: Value, **kwargs: Any):
        return await self._backend.set_raw(key, value, **kwargs)

    async def get_raw(self, key: Key) -> Value:
        return await self._backend.get_raw(key)

    async def get_bits(self, key: Key, *indexes: int, size: int = 1) -> Tuple[int, ...]:
        return await self._backend.get_bits(key, *indexes, size=size)

    async def incr_bits(self, key: Key, *indexes: int, size: int = 1, by: int = 1) -> Tuple[int, ...]:
        return await self._backend.incr_bits(key, *indexes, size=size, by=by)

    async def slice_incr(self, key: Key, start: int, end: int, maxvalue: int, expire: Optional[float] = None) -> int:
        return await self._backend.slice_incr(key, start, end, maxvalue, expire)

    async def get_size(self, key: Key) -> int:
        return await self._backend.get_size(key)

    async def ping(self, message: Optional[bytes] = None) -> bytes:
        return await self._backend.ping(message)

    async def clear(self):
        self._to_delete = set()
        await self._local_cache.clear()
        return await self._backend.clear()

    async def set_lock(self, key: Key, value: Value, expire: float) -> bool:
        return await self._backend.set_lock(key, value, expire)

    async def is_locked(
        self,
        key: Key,
        wait: Optional[float] = None,
        step: float = 0.1,
    ) -> bool:
        return await self._backend.is_locked(key, wait, step)

    async def unlock(self, key: Key, value: Value) -> bool:
        return await self._backend.unlock(key, value)

    async def set_add(self, key: Key, *values: str, expire: Optional[float] = None):
        return await self._backend.set_add(key, *values, expire=expire)

    async def set_remove(self, key: Key, *values: str):
        return await self._backend.set_remove(key, *values)

    async def set_pop(self, key: Key, count: int = 100) -> Iterable[str]:
        return await self._backend.set_pop(key, count)


class LockTransactionBackend(TransactionBackend):
    __slots__ = [
        "_backend",
        "_local_cache",
        "_to_delete",
        "__disable",
        "_locks",
        "_lock_id",
        "_serializable",
        "_timeout",
    ]

    def __init__(self, backend: Backend, serializable=False, timeout=10):
        super().__init__(backend)
        self._locks = set()
        self._lock_id = uuid4().hex
        self._serializable = serializable
        self._timeout = timeout

    def _get_lock_key(self, key: Key) -> Key:
        if self._serializable:
            return _GLOBAL_LOCK_KEY
        return f"{_LOCK_PREFIX}:{key}"

    async def _lock_updates(self, key: Key):
        lock_key = self._get_lock_key(key)
        if lock_key in self._locks:
            return
        wait = self._timeout
        step = 0.1
        while wait > 0.0:
            wait -= step
            if await self._backend.set_lock(lock_key, self._lock_id, expire=self._timeout):
                self._locks.add(lock_key)
                return
            if lock_key in self._locks:
                return
            await asyncio.sleep(step)
        raise LockedError("probably deadlock or long running transactions")

    async def _unlock_updates(self):
        locks = self._locks
        self._locks = set()
        if locks:
            await asyncio.gather(*[self._backend.unlock(key, self._lock_id) for key in locks])

    async def commit(self):
        try:
            await super().commit()
        finally:
            await self._unlock_updates()

    async def rollback(self):
        try:
            await super().rollback()
        finally:
            await self._unlock_updates()

    async def set(
        self,
        key: Key,
        value: Value,
        expire: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        await self._lock_updates(key)
        return await super().set(key, value, expire=expire, exist=exist)

    async def set_many(self, pairs: Mapping[Key, Value], expire: Optional[float] = None):
        for key in pairs.keys():
            await self._lock_updates(key)
        res = await super().set_many(pairs, expire=expire)
        return res

    async def incr(self, key: Key, value: int = 1, expire: Optional[float] = None) -> int:
        await self._lock_updates(key)
        return await super().incr(key, value, expire=expire)

    async def delete(self, key: Key) -> bool:
        await self._lock_updates(key)
        res = await super().delete(key)
        return res

    async def delete_many(self, *keys: Key):
        for key in keys:
            await self._lock_updates(key)
        res = await super().delete_many(*keys)
        return res

    async def delete_match(self, pattern: str):
        await self._local_cache.delete_match(pattern)
        async for key in self._backend.scan(pattern):
            await self._lock_updates(key)
            self._to_delete.add(key)

    async def expire(self, key: Key, timeout: float):
        await self._lock_updates(key)
        return await super().expire(key, timeout)
