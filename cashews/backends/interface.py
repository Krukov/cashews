import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Mapping, Optional, Tuple
from abc import abstractmethod, ABCMeta

from ..exceptions import LockedError


class Backend(metaclass=ABCMeta):
    name: str = ""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    @property
    def is_init(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def init(self):
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @abstractmethod
    async def set(
        self,
        key: str,
        value: Any,
        expire: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def set_raw(self, key: str, value: Any, **kwargs: Any):
        raise NotImplementedError

    @abstractmethod
    async def get(self, key: str, default: Optional[Any] = None) -> Any:
        raise NotImplementedError

    @abstractmethod
    async def get_raw(self, key: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    async def get_many(self, *keys: str, default: Optional[Any] = None) -> Tuple[Optional[Any], ...]:
        raise NotImplementedError

    @abstractmethod
    async def set_many(self, pairs: Mapping[str, Any], expire: Optional[float] = None):
        raise NotImplementedError

    @abstractmethod
    async def exists(self, key: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def keys_match(self, pattern: str) -> AsyncIterator[str]:
        raise NotImplementedError

    @abstractmethod
    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:
        raise NotImplementedError

    @abstractmethod
    async def incr(self, key: str) -> int:
        raise NotImplementedError

    @abstractmethod
    async def delete(self, key: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def delete_match(self, pattern: str):
        raise NotImplementedError

    @abstractmethod
    async def get_match(
        self, pattern: str, batch_size: int = 100, default: Optional[Any] = None
    ) -> AsyncIterator[Tuple[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def expire(self, key: str, timeout: float):
        raise NotImplementedError

    @abstractmethod
    async def get_expire(self, key: str) -> int:
        raise NotImplementedError

    @abstractmethod
    async def get_bits(self, key: str, *indexes: int, size: int = 1) -> Tuple[int]:
        raise NotImplementedError

    @abstractmethod
    async def incr_bits(self, key: str, *indexes: int, size: int = 1, by: int = 1) -> Tuple[int]:
        raise NotImplementedError

    @abstractmethod
    async def get_size(self, key: str) -> int:
        """
        Return size in bites that allocated by a value for given key
        """
        raise NotImplementedError

    @abstractmethod
    async def ping(self, message: Optional[bytes] = None) -> bytes:
        raise NotImplementedError

    @abstractmethod
    async def clear(self):
        raise NotImplementedError

    @abstractmethod
    async def set_lock(self, key: str, value: Any, expire: float) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def is_locked(
        self,
        key: str,
        wait: Optional[float] = None,
        step: float = 0.1,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def unlock(self, key: str, value: Any) -> bool:
        raise NotImplementedError

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
