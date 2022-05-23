import asyncio
from typing import Any, AsyncIterator, List, Optional, Tuple

from ..interface import Backend
from .client import SafeRedis

try:
    from redis.asyncio import BlockingConnectionPool, Redis
except ImportError:
    from aioredis import BlockingConnectionPool, Redis


_UNLOCK = """
if redis.call("get",KEYS[1]) == ARGV[1] then
    return redis.call("del",KEYS[1])
else
    return 0
end
"""
# pylint: disable=arguments-differ
# pylint: disable=abstract-method


class _Redis(Backend):
    name = "redis"

    def __init__(self, address, safe=True, **kwargs):
        kwargs.pop("local_cache", None)
        kwargs.pop("prefix", None)
        kwargs.setdefault("client_name", "cashews")
        kwargs.setdefault("health_check_interval", 10)
        kwargs.setdefault("max_connections", 10)
        kwargs.setdefault("socket_keepalive", True)
        kwargs.setdefault("retry_on_timeout", False)
        kwargs.setdefault("socket_timeout", 0.1)
        kwargs["decode_responses"] = False

        self._pool_class = kwargs.pop("connection_pool_class", BlockingConnectionPool)
        if self._pool_class == BlockingConnectionPool:
            kwargs["timeout"] = kwargs.pop("wait_for_connection_timeout", 0.1)
        self._client = None
        self._sha = {}
        self._safe = safe
        self._kwargs = kwargs
        self._address = address
        self.__is_init = False
        super().__init__()

    @property
    def is_init(self) -> bool:
        return self.__is_init

    async def init(self):
        if not self._safe:
            client_class = Redis
        else:
            client_class = SafeRedis
        self._client = client_class(connection_pool=self._pool_class.from_url(self._address, **self._kwargs))
        self.__is_init = True

    async def clear(self):
        return await self._client.flushdb()

    async def set(self, key: str, value: Any, expire: Optional[float] = None, exist=None) -> bool:
        nx = xx = None
        if exist is True:
            xx = True
        elif exist is False:
            nx = True
        pexpire = None
        if isinstance(expire, float):
            pexpire = int(expire * 1000)
            expire = None
        return bool(await self._client.set(key, value, ex=expire, px=pexpire, nx=nx, xx=xx))

    async def get_expire(self, key: str) -> int:
        return await self._client.ttl(key)

    async def expire(self, key: str, timeout: Optional[float]) -> int:
        return await self._client.expire(key, int(timeout))

    async def set_lock(self, key: str, value, expire: float) -> bool:
        pexpire = None
        if isinstance(expire, float):
            pexpire = int(expire * 1000)
            expire = None
        return bool(await self._client.set(key, value, ex=expire, px=pexpire, nx=True))

    async def is_locked(
        self,
        key: str,
        wait: Optional[float] = None,
        step: float = 0.1,
    ) -> bool:
        if wait is None:
            return await self.exists(key)
        async with self._client.client() as conn:
            while wait > 0.0:
                if not await conn.exists(key):
                    return False
                wait -= step
                await asyncio.sleep(step)
        return True

    async def unlock(self, key: str, value: Any) -> bool:
        if "UNLOCK" not in self._sha:
            self._sha["UNLOCK"] = await self._client.script_load(_UNLOCK.replace("\n", " "))
        return await self._client.evalsha(self._sha["UNLOCK"], 1, key, value)

    async def delete(self, key: str) -> bool:
        return await self._client.unlink(key)

    async def exists(self, key: str) -> bool:
        return bool(await self._client.exists(key))

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:
        cursor = b"0"
        while cursor:
            cursor, keys = await self._client.scan(cursor, match=pattern, count=batch_size)
            for key in keys:
                yield key.decode()

    async def keys_match(self, pattern: str):
        async for key in self.scan(pattern):
            yield key.encode()

    async def delete_match(self, pattern: str):
        if "*" not in pattern:
            return await self._client.unlink(pattern)
        cursor = b"0"
        while cursor:
            cursor, keys = await self._client.scan(cursor, match=pattern, count=100)
            if not keys:
                continue
            await self._client.unlink(keys[0], *keys[1:])
        return True

    async def get_match(
        self, pattern: str, batch_size: int = 100, default: Optional[Any] = None
    ) -> AsyncIterator[Tuple[str, bytes]]:
        cursor = b"0"
        while cursor:
            cursor, keys = await self._client.scan(cursor, match=pattern, count=batch_size)
            if not keys:
                continue
            keys = [key.decode() for key in keys]
            values = await self.get_many(*keys, default=default)
            for key, value in zip(keys, values):
                yield key, value

    async def get_size(self, key: str) -> int:
        size = await self._client.memory_usage(key) or 0
        return int(size)

    async def get(self, key: str, default=None) -> Any:
        return await self._client.get(key)

    async def get_many(self, *keys: str, default: Optional[Any] = None) -> Tuple[Any]:
        if not keys:
            return tuple()
        values = await self._client.mget(keys[0], *keys[1:])
        if not values:
            return []
        return tuple(value if value is not None else default for value in values)

    async def incr(self, key: str) -> int:
        return await self._client.incr(key)

    async def get_bits(self, key: str, *indexes: int, size: int = 1):
        """
        https://redis.io/commands/bitfield
        """
        bitops = self._client.bitfield(key)
        for index in indexes:
            bitops.get(fmt=f"u{size}", offset=f"#{index}")
        return tuple(await bitops.execute())

    async def incr_bits(self, key: str, *indexes: int, size: int = 1, by: int = 1):
        bitops = self._client.bitfield(key)
        for index in indexes:
            bitops.incrby(fmt=f"u{size}", offset=f"#{index}", increment=by, overflow="SAT")
        return tuple(await bitops.execute())

    async def ping(self, message: Optional[bytes] = None) -> bytes:
        pong = await self._client.ping()
        if pong and message:
            return message
        return b"PONG"

    async def set_raw(self, key: str, value: Any, **kwargs):
        return await self._client.set(key, value, **kwargs)

    async def get_raw(self, key: str) -> Any:
        return await self._client.get(key)

    def close(self):
        self._client = None
        self.__is_init = False
