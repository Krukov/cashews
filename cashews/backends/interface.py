import uuid
from abc import ABCMeta, abstractmethod
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Mapping, Optional, Set, Tuple

from cashews.commands import ALL, Command
from cashews.exceptions import LockedError


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
        key: str,
        value: Any,
        expire: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        ...

    @abstractmethod
    async def set_raw(self, key: str, value: Any, **kwargs: Any):
        ...

    @abstractmethod
    async def get(self, key: str, default: Optional[Any] = None) -> Any:
        ...

    @abstractmethod
    async def get_raw(self, key: str) -> Any:
        ...

    @abstractmethod
    async def get_many(self, *keys: str, default: Optional[Any] = None) -> Tuple[Optional[Any], ...]:
        ...

    @abstractmethod
    async def set_many(self, pairs: Mapping[str, Any], expire: Optional[float] = None):
        ...

    @abstractmethod
    async def exists(self, key: str) -> bool:
        ...

    @abstractmethod
    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:
        ...

    @abstractmethod
    async def incr(self, key: str) -> int:
        ...

    @abstractmethod
    async def delete(self, key: str) -> bool:
        ...

    @abstractmethod
    async def delete_many(self, *keys: str):
        ...

    @abstractmethod
    async def delete_match(self, pattern: str):
        ...

    @abstractmethod
    async def get_match(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Tuple[str, Any]]:
        ...

    @abstractmethod
    async def expire(self, key: str, timeout: float):
        ...

    @abstractmethod
    async def get_expire(self, key: str) -> int:
        ...

    @abstractmethod
    async def get_bits(self, key: str, *indexes: int, size: int = 1) -> Tuple[int, ...]:
        ...

    @abstractmethod
    async def incr_bits(self, key: str, *indexes: int, size: int = 1, by: int = 1) -> Tuple[int, ...]:
        ...

    @abstractmethod
    async def slice_incr(self, key: str, start: int, end: int, maxvalue: int, expire: Optional[float] = None) -> int:
        ...

    @abstractmethod
    async def get_size(self, key: str) -> int:
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
    async def set_lock(self, key: str, value: Any, expire: float) -> bool:
        ...

    @abstractmethod
    async def is_locked(
        self,
        key: str,
        wait: Optional[float] = None,
        step: float = 0.1,
    ) -> bool:
        ...

    @abstractmethod
    async def unlock(self, key: str, value: Any) -> bool:
        ...

    @asynccontextmanager
    async def lock(self, key: str, expire: float):
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
