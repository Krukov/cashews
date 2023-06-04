import asyncio
import uuid
from abc import ABCMeta, abstractmethod
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Iterable, List, Mapping, Optional, Set, Tuple

from cashews._typing import Callback, Key, Value
from cashews.commands import ALL, Command
from cashews.exceptions import CacheBackendInteractionError, LockedError

NOT_EXIST = -2
UNLIMITED = -1


class _BackendInterface(metaclass=ABCMeta):
    @property
    @abstractmethod
    def is_init(self) -> bool:
        ...

    @abstractmethod
    async def init(self):
        ...

    @abstractmethod
    async def close(self):
        ...

    @abstractmethod
    async def set(
        self,
        key: Key,
        value: Value,
        expire: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        ...

    @abstractmethod
    async def set_many(self, pairs: Mapping[Key, Value], expire: Optional[float] = None):
        ...

    @abstractmethod
    async def set_raw(self, key: Key, value: Value, **kwargs: Any):
        ...

    @abstractmethod
    async def get(self, key: Key, default: Optional[Value] = None) -> Value:
        ...

    @abstractmethod
    async def get_raw(self, key: Key) -> Value:
        ...

    @abstractmethod
    async def get_many(self, *keys: Key, default: Optional[Value] = None) -> Tuple[Optional[Value], ...]:
        ...

    @abstractmethod
    async def get_match(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Tuple[Key, Value]]:
        ...

    @abstractmethod
    async def exists(self, key: Key) -> bool:
        ...

    @abstractmethod
    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Key]:
        ...

    @abstractmethod
    async def incr(self, key: Key, value: int = 1, expire: Optional[float] = None) -> int:
        ...

    @abstractmethod
    async def delete(self, key: Key) -> bool:
        ...

    @abstractmethod
    async def delete_many(self, *keys: Key):
        ...

    @abstractmethod
    async def delete_match(self, pattern: str):
        ...

    @abstractmethod
    async def expire(self, key: Key, timeout: float):
        ...

    @abstractmethod
    async def get_expire(self, key: Key) -> int:
        ...

    @abstractmethod
    async def get_bits(self, key: Key, *indexes: int, size: int = 1) -> Tuple[int, ...]:
        ...

    @abstractmethod
    async def incr_bits(self, key: Key, *indexes: int, size: int = 1, by: int = 1) -> Tuple[int, ...]:
        ...

    @abstractmethod
    async def slice_incr(self, key: Key, start: int, end: int, maxvalue: int, expire: Optional[float] = None) -> int:
        ...

    @abstractmethod
    async def set_add(self, key: Key, *values: str, expire: Optional[float] = None):
        ...

    @abstractmethod
    async def set_remove(self, key: Key, *values: str):
        ...

    @abstractmethod
    async def set_pop(self, key: Key, count: int = 100) -> Iterable[str]:
        ...

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
    async def ping(self, message: Optional[bytes] = None) -> bytes:
        ...

    @abstractmethod
    async def clear(self):
        ...

    async def set_lock(self, key: Key, value: Value, expire: float) -> bool:
        return await self.set(key, value, expire=expire, exist=False)

    @abstractmethod
    async def is_locked(
        self,
        key: Key,
        wait: Optional[float] = None,
        step: float = 0.1,
    ) -> bool:
        ...

    @abstractmethod
    async def unlock(self, key: Key, value: Value) -> bool:
        ...

    @asynccontextmanager
    async def lock(self, key: Key, expire: float, wait=True):
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


class ControlMixin:
    def __init__(self, *args, **kwargs) -> None:
        self.__disable: ContextVar[Set[Command]] = ContextVar(str(id(self)), default=set())
        super().__init__(*args, **kwargs)

    @property
    def _disable(self) -> Set[Command]:
        return self.__disable.get()

    def _set_disable(self, value: Set[Command]) -> None:
        self.__disable.set(value)

    def is_disable(self, *cmds: Command) -> bool:
        _disable = self._disable
        if not cmds and _disable:
            return True
        for cmd in cmds:
            if cmd in _disable:
                return True
        return False

    def is_enable(self, *cmds: Command) -> bool:
        return not self.is_disable(*cmds)

    @property
    def is_full_disable(self):
        return self._disable == ALL

    def disable(self, *cmds: Command) -> None:
        if not cmds:
            _disable = ALL.copy()
        else:
            _disable = self._disable.copy()
            _disable.update(cmds)
        self._set_disable(_disable)

    def enable(self, *cmds: Command) -> None:
        if not cmds:
            _disable = set()
        else:
            _disable = self._disable.copy()
            _disable -= set(cmds)
        self._set_disable(_disable)


class Backend(ControlMixin, _BackendInterface, metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self._on_remove_callbacks: List[Callback] = []

    def on_remove_callback(self, callback: Callback):
        self._on_remove_callbacks.append(callback)

    async def _call_on_remove_callbacks(self, *keys: Key):
        for callback in self._on_remove_callbacks:
            await callback(keys, backend=self)
