from __future__ import annotations

import asyncio
import uuid
from abc import ABCMeta, abstractmethod
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, AsyncIterator, Iterable, Mapping, overload

from cashews.exceptions import CacheBackendInteractionError, LockedError
from cashews.serialize import Serializer

if TYPE_CHECKING:  # pragma: no cover
    from cashews._typing import Default, Key, OnRemoveCallback, Value

NOT_EXIST = -2
UNLIMITED = -1


class _BackendInterface(metaclass=ABCMeta):
    @property
    @abstractmethod
    def is_init(self) -> bool: ...

    @abstractmethod
    async def init(self): ...

    @abstractmethod
    async def close(self): ...

    @abstractmethod
    async def set(
        self,
        key: Key,
        value: Value,
        expire: float | None = None,
        exist: bool | None = None,
    ) -> bool: ...

    @abstractmethod
    async def set_many(self, pairs: Mapping[Key, Value], expire: float | None = None) -> None: ...

    @abstractmethod
    async def set_raw(self, key: Key, value: Value, **kwargs: Any) -> None: ...

    @overload
    async def get(self, key: Key, default: Default) -> Value | Default: ...

    @overload
    async def get(self, key: Key, default: None = None) -> Value | None: ...

    @abstractmethod
    async def get(self, key: Key, default: Default | None = None) -> Value | Default | None: ...

    @abstractmethod
    async def get_raw(self, key: Key) -> Value: ...

    @abstractmethod
    async def get_many(self, *keys: Key, default: Value | None = None) -> tuple[Value | None, ...]: ...

    @abstractmethod
    def get_match(self, pattern: str, batch_size: int = 100) -> AsyncIterator[tuple[Key, Value]]: ...

    @abstractmethod
    async def exists(self, key: Key) -> bool: ...

    @abstractmethod
    def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Key]: ...

    @abstractmethod
    async def incr(self, key: Key, value: int = 1, expire: float | None = None) -> int: ...

    @abstractmethod
    async def delete(self, key: Key) -> bool: ...

    @abstractmethod
    async def delete_many(self, *keys: Key) -> None: ...

    @abstractmethod
    async def delete_match(self, pattern: str) -> None: ...

    @abstractmethod
    async def expire(self, key: Key, timeout: float): ...

    @abstractmethod
    async def get_expire(self, key: Key) -> int: ...

    @abstractmethod
    async def get_bits(self, key: Key, *indexes: int, size: int = 1) -> tuple[int, ...]: ...

    @abstractmethod
    async def incr_bits(self, key: Key, *indexes: int, size: int = 1, by: int = 1) -> tuple[int, ...]: ...

    @abstractmethod
    async def slice_incr(
        self,
        key: Key,
        start: int | float,
        end: int | float,
        maxvalue: int,
        expire: float | None = None,
    ) -> int: ...

    @abstractmethod
    async def set_add(self, key: Key, *values: str, expire: float | None = None) -> None: ...

    @abstractmethod
    async def set_remove(self, key: Key, *values: str) -> None: ...

    @abstractmethod
    async def set_pop(self, key: Key, count: int = 100) -> Iterable[str]: ...

    @abstractmethod
    async def get_size(self, key: Key) -> int:
        """
        Return size in bites that allocated by a value for given key
        """
        ...

    @abstractmethod
    async def get_keys_count(self) -> int:
        """
        Return count keys in cache
        """
        ...

    @abstractmethod
    async def ping(self, message: bytes | None = None) -> bytes: ...

    @abstractmethod
    async def clear(self) -> None: ...

    async def set_lock(self, key: Key, value: Value, expire: float) -> bool:
        return await self.set(key, value, expire=expire, exist=False)

    @abstractmethod
    async def is_locked(
        self,
        key: Key,
        wait: float | None = None,
        step: float = 0.1,
    ) -> bool: ...

    @abstractmethod
    async def unlock(self, key: Key, value: Value) -> bool: ...

    @asynccontextmanager
    async def lock(self, key: Key, expire: float, wait: bool = True) -> AsyncGenerator[None, None]:
        identifier = str(uuid.uuid4())
        while True:
            lock = await self.set_lock(key, identifier, expire=expire)
            if not lock:
                # we need to check the connection by ping because
                # if redis unavailable and a backend have flag `safe`
                # we will have a brake lock
                try:
                    if await self.ping(b"LOCK") is None:
                        yield
                        return
                except CacheBackendInteractionError:
                    yield
                    return

                if wait:
                    await asyncio.sleep(0)
                    continue
                raise LockedError(f"Key {key} is already locked")
            try:
                yield
            finally:
                await self.unlock(key, identifier)
            return


class Backend(_BackendInterface, metaclass=ABCMeta):
    def __init__(self, *args, serializer: Serializer | None = None, **kwargs) -> None:
        super().__init__()
        self._id = uuid.uuid4().hex
        self._serializer = serializer
        self._on_remove_callbacks: list[OnRemoveCallback] = []

    def on_remove_callback(self, callback: OnRemoveCallback) -> None:
        self._on_remove_callbacks.append(callback)

    async def _call_on_remove_callbacks(self, *keys: Key) -> None:
        for callback in self._on_remove_callbacks:
            await callback(keys, backend=self)
