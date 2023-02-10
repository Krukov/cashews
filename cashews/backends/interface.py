import uuid
from abc import ABCMeta, abstractmethod
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Mapping, Optional, Set, Tuple

from cashews._typing import Key, Tags, Value
from cashews.commands import ALL, Command
from cashews.exceptions import LockedError

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
        tags: Optional[Tags] = None,
    ) -> bool:
        ...

    @abstractmethod
    async def set_many(self, pairs: Mapping[Key, Value], expire: Optional[float] = None, tags: Optional[Tags] = None):
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
    async def exists(self, key: Key) -> bool:
        ...

    @abstractmethod
    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Key]:
        ...

    @abstractmethod
    async def incr(self, key: Key, value: int = 1, tags: Optional[Tags] = None) -> int:
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

    # @abstractmethod
    async def delete_tags(self, tags: Tags):
        ...

    @abstractmethod
    async def get_match(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Tuple[Key, Value]]:
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
    async def get_size(self, key: Key) -> int:
        """
        Return size in bites that allocated by a value for given key
        """
        ...

    @abstractmethod
    async def ping(self, message: Optional[bytes] = None) -> bytes:
        ...

    @abstractmethod
    async def clear(self):
        ...

    @abstractmethod
    async def set_lock(self, key: Key, value: Value, expire: float) -> bool:
        ...

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
    async def lock(self, key: Key, expire: float):
        identifier = str(uuid.uuid4())
        lock = await self.set_lock(key, identifier, expire=expire)
        if not lock:
            # we need to check the connection by ping because
            # if for example a redis unavailable and a backend have flag `safe`
            # we will have a brake lock
            try:
                await self.ping(b"TEST")
            except Exception:
                pass
            else:
                raise LockedError(f"Key {key} already locked")
        try:
            yield
        finally:
            await self.unlock(key, identifier)


class ControlMixin:
    def __init__(self) -> None:
        self.__disable: ContextVar[Set[Command]] = ContextVar(str(id(self)), default=set())

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
