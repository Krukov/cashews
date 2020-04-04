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

    async def get(self, key: str) -> Any:
        ...

    async def get_many(self, *keys: str) -> Tuple[Any]:
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

    async def ping(self, message: Optional[str] = None) -> str:
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

    async def set(
        self, key: str, value: Any, expire: Union[None, float, int] = None, exist: Optional[bool] = None
    ) -> bool:
        return await self._target.set(key, value, expire=expire, exist=exist)

    async def get(self, key: str) -> Any:
        return await self._target.get(key)

    async def get_many(self, *keys: str) -> Tuple[Any]:
        return await self._target.get_many(keys)

    async def incr(self, key: str) -> int:
        return await self._target.incr(key)

    async def delete(self, key: str):
        return await self._target.delete(key)

    async def delete_match(self, pattern: str):
        return await self._target.delete_match(pattern)

    async def expire(self, key: str, timeout: Union[int, float]):
        return await self._target.expire(key, timeout)

    async def get_expire(self, key: str) -> int:
        return await self._target.get_expire(key)

    async def ping(self, message: Optional[str] = None) -> str:
        if message is not None:
            return await self._target.ping(message)
        return await self._target.ping()

    async def clear(self):
        return await self._target.clear()

    async def close(self):
        return await self._target.close()

    async def set_lock(self, key: str, value: Any, expire: Union[float, int]) -> bool:
        return await self._target.set_lock(key, value, expire)

    async def is_locked(self, key: str, wait: Union[int, float, None] = None, step: Union[int, float] = 0.1) -> bool:
        return await self._target.is_locked(key, wait=wait, step=step)

    async def unlock(self, key: str, value: str) -> bool:
        return await self._target.unlock(key, value)
