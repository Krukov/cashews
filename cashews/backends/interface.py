import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional, Tuple


class LockedException(Exception):
    pass


class Backend:
    name: str = ""
    is_init: bool = False

    def __init__(self, *args, **kwargs):
        ...

    async def init(self):
        ...

    def close(self):
        ...

    async def set(
        self,
        key: str,
        value: Any,
        expire: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        ...

    async def set_raw(self, key: str, value: Any, **kwargs):
        ...

    async def get(self, key: str, default: Optional[Any] = None) -> Any:
        ...

    async def get_raw(self, key: str) -> Any:
        ...

    async def get_many(self, *keys: str, default: Optional[Any] = None) -> Tuple[Any]:
        ...

    async def exists(self, key: str) -> bool:
        ...

    async def keys_match(self, pattern: str):
        ...

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:
        ...

    async def incr(self, key: str) -> int:
        ...

    async def delete(self, key: str) -> bool:
        ...

    async def delete_match(self, pattern: str):
        ...

    async def get_match(
        self, pattern: str, batch_size: int = 100, default: Optional[Any] = None
    ) -> AsyncIterator[Tuple[str, Any]]:
        ...

    async def expire(self, key: str, timeout: float):
        ...

    async def get_expire(self, key: str) -> int:
        ...

    async def get_bits(self, key: str, *indexes: int, size: int = 1) -> Tuple[int]:
        ...

    async def incr_bits(self, key: str, *indexes: int, size: int = 1, by: int = 1) -> Tuple[int]:
        ...

    async def get_size(self, key: str) -> int:
        """
        Return size in bites that allocated by a value for given key
        """
        ...

    async def ping(self, message: Optional[bytes] = None) -> bytes:
        ...

    async def clear(self):
        ...

    async def set_lock(self, key: str, value: Any, expire: float) -> bool:
        ...

    async def is_locked(
        self,
        key: str,
        wait: Optional[float] = None,
        step: float = 0.1,
    ) -> bool:
        ...

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
                raise LockedException(f"Key {key} already locked")
        try:
            yield
        finally:
            await self.unlock(key, identifier)
