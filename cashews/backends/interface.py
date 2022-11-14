import uuid
from abc import ABCMeta, abstractmethod
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Mapping, Optional, Tuple

from ..disable_control import ControlMixin
from ..exceptions import LockedError


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


class Backend(ControlMixin, _BackendInterface, metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        super().__init__()
