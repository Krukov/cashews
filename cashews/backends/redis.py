import asyncio
import logging
import socket
from typing import Any, Optional, Union

from aioredis import ConnectionsPool as __ConnectionsPool
from aioredis import Redis as Redis_
from aioredis import RedisError, create_pool

from ..serialize import PickleSerializerMixin
from .interface import Backend

__all__ = "SafeRedis"
logger = logging.getLogger(__name__)
_UNLOCK = """
if redis.call("get",KEYS[1]) == ARGV[1] then
    return redis.call("del",KEYS[1])
else
    return 0
end
"""
# pylint: disable=arguments-differ
# pylint: disable=abstract-method


class _Redis(Redis_):
    def __init__(self, address, safe=False, **kwargs):
        self._address = address
        kwargs.pop("local_cache", None)
        kwargs.pop("prefix", None)
        self._kwargs = kwargs
        self._pool_or_conn: Optional[_ConnectionsPool] = None
        self._sha = {}
        self._safe = safe

    @property
    def is_init(self):
        return bool(self._pool_or_conn)

    async def init(self):
        try:
            self._pool_or_conn = await create_pool(address=self._address, pool_cls=_ConnectionsPool, **self._kwargs)
        except Exception:
            if not self._safe:
                raise

    def get_many(self, *keys: str):
        return self.mget(keys[0], *keys[1:])

    def clear(self):
        return self.flushdb()

    async def set(self, key: str, value: Any, expire: Union[None, float, int] = None, exist=None):
        if exist is True:
            exist = Redis_.SET_IF_EXIST
        elif exist is False:
            exist = Redis_.SET_IF_NOT_EXIST
        pexpire = None
        if isinstance(expire, float):
            pexpire = int(expire * 1000)
            expire = None
        return await super().set(key, value, expire=expire, pexpire=pexpire, exist=exist)

    def get_expire(self, key: str) -> int:
        return self.ttl(key)

    def set_lock(self, key: str, value, expire):
        pexpire = None
        if isinstance(expire, float):
            pexpire = int(expire * 1000)
            expire = None
        return super().set(key, value, expire=expire, pexpire=pexpire, exist=self.SET_IF_NOT_EXIST)

    async def is_locked(self, key: str, wait=None, step=0.1):
        if wait is None:
            return await self.exists(key)
        while wait > 0.0:
            if not await self.exists(key):
                return False
            wait -= step
            await asyncio.sleep(step)
        return True

    async def unlock(self, key, value):
        if "UNLOCK" not in self._sha:
            self._sha["UNLOCK"] = await self.script_load(_UNLOCK.replace("\n", " "))
        return await self.evalsha(self._sha["UNLOCK"], keys=[key], args=[value])

    def delete(self, key):
        return self.unlink(key)

    def exists(self, key) -> bool:
        return super().exists(key)

    async def keys_match(self, pattern: str):
        cursor = b"0"
        while cursor:
            cursor, keys = await self.scan(cursor, match=pattern, count=100)
            for key in keys:
                yield key

    async def delete_match(self, pattern: str):
        if "*" not in pattern:
            return await self.unlink(pattern)
        keys = []
        async for key in self.keys_match(pattern):
            keys.append(key)
        if keys:
            return await self.unlink(keys[0], *keys[1:])

    async def get_size(self, key: str) -> int:
        return int(await self.execute(b"MEMORY", b"USAGE", key))

    async def get(self, key: str, **kwargs) -> Any:
        return await super().get(key=key, **kwargs)

    async def execute(self, command, *args, **kwargs):
        try:
            return await super().execute(command, *args, **kwargs)
        except (RedisError, socket.gaierror, OSError, asyncio.TimeoutError, AttributeError):
            if not self._safe or command.lower() == b"ping":
                raise
            logger.error("Redis down on command %s", command)
            if command.lower() in [b"unlink", b"del", b"memory", b"ttl"]:
                return 0
            if command.lower() == b"scan":
                return [0, []]
            return None


class Redis(PickleSerializerMixin, _Redis, Backend):
    pass


class _ConnectionsPool(__ConnectionsPool):
    async def _create_new_connection(self, address):
        conn = await super()._create_new_connection(address)
        await conn.execute(b"CLIENT", b"SETNAME", b"cachews")
        return conn
