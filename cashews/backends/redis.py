import asyncio
import logging
import socket
from typing import Any, Optional, Union

from aioredis import ConnectionsPool as _ConnectionsPool
from aioredis import Redis as Redis_
from aioredis import RedisError, util

from ..key import get_template_and_func_for
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
    _GET_COUNT = """
        local res = redis.call('get',KEYS[1])
        if res then
            redis.call('incr','_counters:'..ARGV[1]..':hit')
        else
            redis.call('incr','_counters:'..ARGV[1]..':miss')
        end
        return res
    """

    def __init__(self, address, safe=False, count_stat=False, **kwargs):
        self._address = address
        self._kwargs = kwargs
        self._pool_or_conn: Optional[_ConnectionsPool] = None
        self._sha = {}
        self._safe = safe
        self._count_stat = count_stat

    async def init(self):
        pool = create_pool(address=self._address, **self._kwargs)

        try:
            await pool._fill_free(override_min=False)
        except Exception:
            if not self._safe:
                pool.close()
                await pool.wait_closed()
                raise

        self._pool_or_conn = pool

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
        if self._count_stat:
            template, _ = get_template_and_func_for(key)
            if template:
                return (
                    await asyncio.gather(
                        super().set(key, value, expire=expire, pexpire=pexpire, exist=exist),
                        self.incr(f"_counters:{template}:set"),
                    )
                )[0]
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
        if not self._count_stat:
            return await super().get(key=key, **kwargs)
        template, _ = get_template_and_func_for(key)
        if template:
            if "GET" not in self._sha:
                self._sha["GET"] = await self.script_load(self._GET_COUNT.replace("\n", " "))
            return await self.evalsha(self._sha["GET"], keys=[key], args=[template])
        return await super().get(key=key, **kwargs)

    async def get_counters(self, template):
        get = super().get
        _hit, _miss, _set = await asyncio.gather(
            get(f"_counters:{template}:hit"), get(f"_counters:{template}:miss"), get(f"_counters:{template}:set"),
        )
        return {"hit": int(_hit or 0), "miss": int(_miss or 0), "set": int(_set or 0)}

    async def execute(self, command, *args, **kwargs):
        try:
            return await super().execute(command, *args, **kwargs)
        except (RedisError, socket.gaierror, OSError, asyncio.TimeoutError):
            if not self._safe:
                raise
            logger.error("Redis down on command %s", command)
            if command.lower() in [b"unlink", b"del", b"memory"]:
                return 0
            if command.lower() == b"scan":
                return [0, []]
            return None


class Redis(PickleSerializerMixin, _Redis, Backend):
    pass


class __ConnectionsPool(_ConnectionsPool):
    async def _create_new_connection(self, address):
        conn = await super()._create_new_connection(address)
        await conn.execute(b"CLIENT", b"SETNAME", b"cachews")
        return conn


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
    connection_cls=None,
):
    if pool_cls:
        cls = pool_cls
    else:
        cls = __ConnectionsPool
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
