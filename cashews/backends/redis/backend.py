import asyncio
import warnings
from typing import Any, AsyncIterator, Dict, Iterable, Mapping, Optional, Tuple, Type, Union

from redis.asyncio import BlockingConnectionPool
from redis.asyncio.client import Pipeline

from cashews._typing import Key, Value
from cashews.backends.interface import Backend

from .client import Redis, SafePipeline, SafeRedis

_UNLOCK = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""
_INCR_SLICE = """
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, "(" .. ARGV[1])
local current_count = redis.call("ZCOUNT", KEYS[1], ARGV[1], ARGV[2])
if current_count < tonumber(ARGV[3]) then
    current_count = current_count + 1
    redis.call("ZADD", KEYS[1], ARGV[2], ARGV[2])
    if tonumber(ARGV[4]) > 0 then
        redis.call("PEXPIRE", KEYS[1], ARGV[4])
    end
end
return current_count
"""
_INCR_EXPIRE = """
local current_count = redis.call("INCRBY", KEYS[1], ARGV[1])
if current_count == 1 then
    redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return current_count
"""
_empty = object()
# pylint: disable=arguments-differ
# pylint: disable=abstract-method


class _Redis(Backend):
    _client: Union[Redis, SafeRedis]
    _client_class: Union[Type[Redis], Type[SafeRedis]]

    def __init__(self, address: str, safe: bool = _empty, suppress: bool = True, **kwargs: Any) -> None:
        if safe is not _empty:
            warnings.warn(
                "`safe` property was renamed to `suppress` and will be removed in next release",
                DeprecationWarning,
            )
            suppress = safe

        kwargs.pop("local_cache", None)
        kwargs.pop("prefix", None)
        kwargs.setdefault("client_name", "cashews")
        kwargs.setdefault("health_check_interval", 10)
        kwargs.setdefault("max_connections", 10)
        kwargs.setdefault("retry_on_timeout", False)
        kwargs.setdefault("socket_timeout", 1)
        if not address.startswith("unix"):
            kwargs.setdefault("socket_keepalive", True)
        kwargs["decode_responses"] = False

        self._pool_class = kwargs.pop("connection_pool_class", BlockingConnectionPool)
        if self._pool_class == BlockingConnectionPool:
            kwargs["timeout"] = kwargs.pop("wait_for_connection_timeout", 10)
        self._sha: Dict[str, Any] = {}
        if not suppress:
            self._client_class = Redis
            self._pipeline_class = Pipeline
        else:
            self._pipeline_class = SafePipeline
            self._client_class = SafeRedis
        self._kwargs = kwargs
        self._address = address
        self.__is_init = False
        super().__init__()

    @property
    def is_init(self) -> bool:
        return self.__is_init

    async def init(self):
        self._client = self._client_class(connection_pool=self._pool_class.from_url(self._address, **self._kwargs))
        await self._client.initialize()
        self.__is_init = True

    @property
    def _pipeline(self):
        return self._pipeline_class(self._client.connection_pool, self._client.response_callbacks, True, None)

    async def clear(self):
        return await self._client.flushdb()

    async def set(
        self,
        key: Key,
        value: Value,
        expire: Optional[float] = None,
        exist=None,
    ) -> bool:
        nx = xx = None
        if exist is True:
            xx = True
        elif exist is False:
            nx = True
        px = int(expire * 1000) if expire else None
        _set = bool(await self._client.set(key, value, px=px, nx=nx, xx=xx))
        return _set

    async def set_many(self, pairs: Mapping[Key, Value], expire: Optional[float] = None):
        px = int(expire * 1000) if expire else None
        async with self._pipeline as pipe:
            for key, value in pairs.items():
                await pipe.set(key, value, px=px)
            await pipe.execute()

    async def get_expire(self, key: Key) -> int:
        return await self._client.ttl(key)

    async def expire(self, key: Key, timeout: float):
        return await self._client.pexpire(key, int(timeout * 1000))

    async def set_lock(self, key: Key, value: Value, expire: float) -> bool:
        pexpire = int(expire * 1000)
        return bool(await self._client.set(key, value, px=pexpire, nx=True))

    async def is_locked(
        self,
        key: Key,
        wait: Optional[float] = None,
        step: float = 0.1,
    ) -> bool:
        if wait is None:
            return await self.exists(key)
        while wait > 0.0:
            if not await self.exists(key):
                return False
            wait -= step
            await asyncio.sleep(step)
        return True

    async def unlock(self, key: Key, value: Value) -> bool:
        if "UNLOCK" not in self._sha:
            self._sha["UNLOCK"] = await self._client.script_load(_UNLOCK.replace("\n", " "))
        return await self._client.evalsha(self._sha["UNLOCK"], 1, key, value)

    async def delete(self, key: Key) -> bool:
        try:
            return bool(await self._client.unlink(key))
        finally:
            await self._call_on_remove_callbacks(key)

    async def exists(self, key: Key) -> bool:
        return bool(await self._client.exists(key))

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Key]:  # type: ignore
        cursor = 0
        while True:
            cursor, keys = await self._client.scan(cursor, match=pattern, count=batch_size)
            for key in keys:
                yield key.decode()
            if not cursor:
                return

    async def delete_many(self, *keys: Key):
        try:
            await self._client.unlink(*keys)
        finally:
            await self._call_on_remove_callbacks(*keys)

    async def delete_match(self, pattern: str):
        if "*" not in pattern:
            await self._client.unlink(pattern)
            return
        cursor = 0
        while True:
            cursor, keys = await self._client.scan(cursor, match=pattern, count=100)
            if not keys:
                if not cursor:
                    return
                continue
            await self._client.unlink(*keys)
            await self._call_on_remove_callbacks(*[key.decode() for key in keys])

    async def get_match(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Tuple[Key, Value]]:  # type: ignore
        cursor = 0
        while True:
            cursor, keys = await self._client.scan(cursor, match=pattern, count=batch_size)
            if not keys:
                if not cursor:
                    return
                continue
            keys = [key.decode() for key in keys]
            values = await self.get_many(*keys, default=_empty)
            for key, value in zip(keys, values):
                if value is not _empty:  # key can be deleted after scan
                    yield key, value
            if not cursor:
                return

    async def get_size(self, key: Key) -> int:
        size = await self._client.memory_usage(key) or 0
        return int(size)

    async def get(self, key: Key, default: Optional[Value] = None) -> Value:
        value = await self._client.get(key)
        return self._transform_value(value, default)

    async def get_many(self, *keys: Key, default: Optional[Value] = None) -> Tuple[Optional[Value], ...]:
        if not keys:
            return tuple()
        values = await self._client.mget(*keys)
        if values is None:
            return tuple([default] * len(keys))
        return tuple(self._transform_value(value, default) for value in values)

    @staticmethod
    def _transform_value(value: Optional[bytes], default: Optional[Value]):
        if value is None:
            return default
        if value.isdigit():
            return int(value)
        return value

    async def incr(self, key: Key, value: int = 1, expire: Optional[float] = None) -> int:
        if not expire:
            return await self._client.incr(key, amount=value)
        if "INCR_EXPIRE" not in self._sha:
            self._sha["INCR_EXPIRE"] = await self._client.script_load(_INCR_EXPIRE.replace("\n", " "))
        expire = expire or 0
        expire = int(expire * 1000)
        return await self._client.evalsha(self._sha["INCR_EXPIRE"], 1, key, value, expire)

    async def get_bits(self, key: Key, *indexes: int, size: int = 1) -> Tuple[int, ...]:
        """
        https://redis.io/commands/bitfield
        """
        bitops = self._client.bitfield(key)
        for index in indexes:
            bitops.get(fmt=f"u{size}", offset=f"#{index}")
        return tuple(await bitops.execute() or [])

    async def incr_bits(self, key: Key, *indexes: int, size: int = 1, by: int = 1) -> Tuple[int, ...]:
        bitops = self._client.bitfield(key)
        for index in indexes:
            bitops.incrby(fmt=f"u{size}", offset=f"#{index}", increment=by, overflow="SAT")
        return tuple(await bitops.execute())

    async def ping(self, message: Optional[bytes] = None) -> bytes:
        await self._client.ping()
        return b"PONG" if message in (None, b"PING") else message

    async def set_raw(self, key: Key, value: Value, **kwargs: Any):
        return await self._client.set(key, value, **kwargs)

    async def get_raw(self, key: Key) -> Value:
        return await self._client.get(key)

    async def slice_incr(self, key: Key, start: int, end: int, maxvalue: int, expire: Optional[float] = None) -> int:
        expire = expire or 0
        expire = int(expire * 1000)
        if "INCR_SLICE" not in self._sha:
            self._sha["INCR_SLICE"] = await self._client.script_load(_INCR_SLICE.replace("\n", " "))
        return await self._client.evalsha(self._sha["INCR_SLICE"], 1, key, start, end, maxvalue, expire)

    async def set_add(self, key: Key, *values: str, expire: Optional[float] = None):
        if expire is None:
            return await self._client.sadd(key, *values)
        expire = int(expire * 1000)
        async with self._pipeline as pipe:
            await pipe.sadd(key, *values)
            await pipe.pexpire(key, expire)
            await pipe.execute()

    async def set_remove(self, key: Key, *values: str):
        await self._client.srem(key, *values)

    async def set_pop(self, key: Key, count: int = 100) -> Iterable[str]:
        return [value.decode() for value in await self._client.spop(key, count)]

    async def get_keys_count(self) -> int:
        return await self._client.dbsize()

    async def close(self):
        await self._client.close()
        self.__is_init = False
