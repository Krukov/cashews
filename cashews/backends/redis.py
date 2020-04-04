import asyncio
import logging
import socket
from typing import Any, Union

from aioredis import ConnectionsPool
from aioredis import Redis as Redis_
from aioredis import RedisError, util

from ..serialize import PickleSerializerMixin
from .interface import ProxyBackend

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


class _SafeRedis(PickleSerializerMixin, Redis_):
    """
    High-level Redis interface.
    Don't throw exception anyway.
    """

    async def execute(self, command, *args, **kwargs):
        try:
            return await super().execute(command, *args, **kwargs)
        except (RedisError, socket.gaierror, OSError, asyncio.TimeoutError):
            logger.error("Redis down on command %s", command)
            return None


class _Redis(PickleSerializerMixin, Redis_):
    pass


class Redis(ProxyBackend):
    def __init__(self, address, hash_key, safe=False, **kwargs):
        self._safe = safe
        if isinstance(hash_key, str):
            hash_key = hash_key.encode()
        self._hash_key = hash_key
        self._address = address
        self._kwargs = kwargs
        super().__init__()

    async def init(self):
        pool = create_pool(address=self._address, **self._kwargs)
        _target_class = _SafeRedis if self._safe else _Redis

        try:
            await pool._fill_free(override_min=False)
        except Exception:
            if not self._safe:
                pool.close()
                await pool.wait_closed()
                raise

        self._target = _target_class(pool, hash_key=self._hash_key)

    def get_many(self, *keys: str):
        return self._target.mget(keys[0], *keys[1:])

    def set(self, key: str, value: Any, expire: Union[None, float, int] = None, exist=None):
        if exist is True:
            exist = Redis_.SET_IF_EXIST
        elif exist is False:
            exist = Redis_.SET_IF_NOT_EXIST
        pexpire = None
        if isinstance(expire, float):
            pexpire = int(expire * 1000)
            expire = None
        return self._target.set(key, value, expire=expire, pexpire=pexpire, exist=exist)

    def get_expire(self, key: str) -> int:
        return self._target.ttl(key)

    def clear(self):
        return self._target.flushdb()

    def set_lock(self, key: str, value, expire):
        return self.set(key, value, expire=expire, exist=False)

    async def is_locked(self, key: str, wait=None, step=0.1):
        if wait is None:
            return await self._target.exists(key)
        while wait > 0.0:
            if not await self._target.exists(key):
                return False
            wait -= step
            await asyncio.sleep(step)
        return True

    def unlock(self, key, value):
        return self._target.eval(_UNLOCK, keys=[key], args=[value])

    def delete(self, key: str):
        return self._target.unlink(key)

    async def delete_match(self, pattern: str):
        cursor = b"0"
        while cursor:
            cursor, keys = await self._target.scan(cursor, match=pattern)
            if keys:
                await self._target.unlink(*keys)

    async def close(self):
        if self._target:
            self._target.close()


def create_pool(
    address,
    *,
    db=None,
    password=None,
    ssl=None,
    encoding=None,
    minsize=1,
    maxsize=10,
    parser=None,
    loop=None,
    create_connection_timeout=None,
    pool_cls=None,
    connection_cls=None
):
    if pool_cls:
        cls = pool_cls
    else:
        cls = ConnectionsPool
    if isinstance(address, str):
        address, options = util.parse_url(address)
        db = options.setdefault("db", db)
        password = options.setdefault("password", password)
        encoding = options.setdefault("encoding", encoding)
        create_connection_timeout = options.setdefault("timeout", create_connection_timeout)
        if "ssl" in options:
            assert options["ssl"] or (not options["ssl"] and not ssl), (
                "Conflicting ssl options are set",
                options["ssl"],
                ssl,
            )
            ssl = ssl or options["ssl"]

    return cls(
        address,
        db,
        password,
        encoding,
        minsize=minsize,
        maxsize=maxsize,
        ssl=ssl,
        parser=parser,
        create_connection_timeout=create_connection_timeout,
        connection_cls=connection_cls,
        loop=loop,
    )
