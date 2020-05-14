import uuid
from contextlib import asynccontextmanager
from typing import Any, Optional, Tuple, Union


class LockedException(Exception):
    pass


class Backend:
    def __init__(self, *args, **kwargs):
        ...

    async def init(self):
        ...

    async def close(self):
        ...

    async def set(
        self, key: str, value: Any, expire: Union[None, float, int] = None, exist: Optional[bool] = None
    ) -> bool:
        ...

    async def get(self, key: str, default: Optional[Any] = None) -> Any:
        ...

    async def get_many(self, *keys: str) -> Tuple[Any]:
        ...

    async def keys_match(self, pattern: str):
        ...

    async def incr(self, key: str) -> int:
        ...

    async def delete(self, key: str):
        ...

    async def delete_match(self, pattern: str):
        ...

    async def expire(self, key: str, timeout: Union[float, int]):
        ...

    async def get_expire(self, key: str) -> int:
        ...

    async def get_size(self, key: str) -> int:
        """
        Return size in bites that allocated by a value for given key
        """
        ...

    async def get_counters(self, template):
        ...

    async def ping(self, message: Optional[bytes] = None) -> str:
        ...

    async def clear(self):
        ...

    async def set_lock(self, key: str, value: Any, expire: Union[float, int]) -> bool:
        ...

    async def is_locked(self, key: str, wait: Union[None, int, float] = None, step: Union[int, float] = 0.1) -> bool:
        ...

    async def unlock(self, key, value) -> bool:
        ...

    @asynccontextmanager
    async def lock(self, key, expire):
        identifier = str(uuid.uuid4())
        lock = await self.set_lock(key, identifier, expire=expire)
        if not lock and await self.ping(b"TEST") == b"TEST":
            raise LockedException(f"Key {key} already locked")
        try:
            yield
        finally:
            await self.unlock(key, identifier)


class ProxyBackend(Backend):
    def __init__(self, target=None):
        self._target = target
        super().__init__()

    def set(self, key: str, value: Any, expire: Union[None, float, int] = None, exist: Optional[bool] = None) -> bool:
        return self._target.set(key, value, expire=expire, exist=exist)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self._target.get(key, default=default)

    def get_many(self, *keys: str) -> Tuple[Any]:
        return self._target.get_many(keys)

    def incr(self, key: str) -> int:
        return self._target.incr(key)

    def delete(self, key: str):
        return self._target.delete(key)

    def delete_match(self, pattern: str):
        return self._target.delete_match(pattern)

    def expire(self, key: str, timeout: Union[int, float]):
        return self._target.expire(key, timeout)

    def get_expire(self, key: str) -> int:
        return self._target.get_expire(key)

    def ping(self, message: Optional[bytes] = None) -> str:
        if message is not None:
            return self._target.ping(message)
        return self._target.ping()

    def clear(self):
        return self._target.clear()

    def close(self):
        return self._target.close()

    def set_lock(self, key: str, value: Any, expire: Union[float, int]) -> bool:
        return self._target.set_lock(key, value, expire)

    def is_locked(self, key: str, wait: Union[int, float, None] = None, step: Union[int, float] = 0.1) -> bool:
        return self._target.is_locked(key, wait=wait, step=step)

    def unlock(self, key: str, value: str) -> bool:
        return self._target.unlock(key, value)

    def keys_match(self, pattern: str):
        return self._target.keys_match(pattern)

    def get_size(self, key):
        return self._target.get_size(key)
