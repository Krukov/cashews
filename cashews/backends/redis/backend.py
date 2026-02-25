from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator, Iterable, Mapping
from typing import Any

from redis.asyncio import BlockingConnectionPool
from redis.asyncio.client import Pipeline

from cashews._typing import Key, Value
from cashews.backends.interface import Backend
from cashews.serialize import DEFAULT_SERIALIZER, Serializer

from .client import Redis, RedisCluster, SafePipeline, SafeRedis, SafeRedisCluster

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
    _client: Redis | SafeRedis | RedisCluster | SafeRedisCluster
    _client_class: type[Redis] | type[SafeRedis] | type[RedisCluster] | type[SafeRedisCluster]
    _is_cluster: bool

    def __init__(
        self,
        address: str,
        suppress: bool = True,
        cluster: bool = False,
        **kwargs: Any,
    ) -> None:
        kwargs.pop("local_cache", None)
        kwargs.pop("prefix", None)
        kwargs.setdefault("client_name", "cashews")
        kwargs.setdefault("health_check_interval", 10)
        kwargs.setdefault("max_connections", 10)

        self._is_cluster = cluster

        if not cluster:
            kwargs.setdefault("retry_on_timeout", False)
        kwargs.setdefault("socket_timeout", 1)
        if not address.startswith("unix"):
            kwargs.setdefault("socket_keepalive", True)
        kwargs["decode_responses"] = False

        if not cluster:
            self._pool_class = kwargs.pop("connection_pool_class", BlockingConnectionPool)
            if self._pool_class == BlockingConnectionPool:
                kwargs["timeout"] = kwargs.pop("wait_for_connection_timeout", 10)

        self._sha: dict[str, Any] = {}
        if not suppress:
            self._client_class = RedisCluster if cluster else Redis
            self._pipeline_class = Pipeline
        else:
            self._pipeline_class = SafePipeline
            self._client_class = SafeRedisCluster if cluster else SafeRedis
        self._kwargs = kwargs
        self._address = address
        self.__is_init = False
        super().__init__(serializer=kwargs.pop("serializer", None))
        self._serializer: Serializer = self._serializer or DEFAULT_SERIALIZER

    @property
    def is_cluster(self) -> bool:
        return self._is_cluster

    @property
    def is_init(self) -> bool:
        return self.__is_init

    async def init(self):
        if self._is_cluster:
            self._client = self._client_class.from_url(self._address, **self._kwargs)
        else:
            pool = self._pool_class.from_url(self._address, **self._kwargs)
            if hasattr(self._client_class, "from_pool"):
                self._client = self._client_class.from_pool(pool)
            else:
                self._client = self._client_class(connection_pool=pool)
        await self._client.initialize()
        self.__is_init = True

    @property
    def _pipeline(self):
        if self._is_cluster:
            return self._client.pipeline()
        return self._pipeline_class(self._client.connection_pool, self._client.response_callbacks, True, None)

    async def clear(self):
        return await self._client.flushdb()

    async def set(
        self,
        key: Key,
        value: Value,
        expire: float | None = None,
        exist=None,
    ) -> bool:
        value = await self._serializer.encode(self, key=key, value=value, expire=expire)
        nx = xx = False
        if exist is True:
            xx = True
        elif exist is False:
            nx = True
        px = int(expire * 1000) if expire else None
        _set = bool(await self._client.set(key, value, px=px, nx=nx, xx=xx))
        return _set

    async def set_many(self, pairs: Mapping[Key, Value], expire: float | None = None):
        px = int(expire * 1000) if expire else None
        if self._is_cluster:
            slots_map = self._group_pairs_by_slot(pairs)
            for subpairs in slots_map.values():
                encoded = {}
                for key, value in subpairs.items():
                    encoded[key] = await self._serializer.encode(self, key=key, value=value, expire=expire)
                await asyncio.gather(*(self._client.set(k, v, px=px) for k, v in encoded.items()))
            return
        async with self._pipeline as pipe:
            for key, value in pairs.items():
                value = await self._serializer.encode(self, key=key, value=value, expire=expire)
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
        wait: float | None = None,
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
        if self._sha.get("UNLOCK") is None:
            self._sha["UNLOCK"] = self._client.register_script(_UNLOCK.replace("\n", " "))  # type: ignore
        return await self._sha["UNLOCK"](keys=(key,), args=(value,))

    async def delete(self, key: Key) -> bool:
        try:
            return bool(await self._client.unlink(key))
        finally:
            await self._call_on_remove_callbacks(key)

    async def exists(self, key: Key) -> bool:
        return bool(await self._client.exists(key))

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Key]:
        if self._is_cluster:
            async for key in self._client.scan_iter(match=pattern, count=batch_size):
                yield key.decode()
            return

        cursor = 0
        while True:
            cursor, keys = await self._client.scan(cursor, match=pattern, count=batch_size)
            for key in keys:
                yield key.decode()
            if not cursor:
                return

    async def delete_many(self, *keys: Key):
        try:
            if self._is_cluster:
                slots_map = self._group_keys_by_slot(keys)
                for subkeys in slots_map.values():
                    if subkeys:
                        await self._client.unlink(*subkeys)
            else:
                await self._client.unlink(*keys)
        finally:
            await self._call_on_remove_callbacks(*keys)

    async def delete_match(self, pattern: str):
        if "*" not in pattern:
            await self._client.unlink(pattern)
            return

        if self._is_cluster:
            batch = []
            async for key in self._client.scan_iter(match=pattern, count=100):
                batch.append(key)
                if len(batch) >= 1000:
                    await self._client.unlink(*batch)
                    await self._call_on_remove_callbacks(*[k.decode() for k in batch])
                    batch = []
            if batch:
                await self._client.unlink(*batch)
                await self._call_on_remove_callbacks(*[k.decode() for k in batch])
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

    async def get_match(self, pattern: str, batch_size: int = 100) -> AsyncIterator[tuple[Key, Value]]:
        if self._is_cluster:
            keys_buf: list[str] = []
            async for bkey in self._client.scan_iter(match=pattern, count=batch_size):
                keys_buf.append(bkey.decode())
                if len(keys_buf) >= batch_size:
                    values = await self.get_many(*keys_buf, default=_empty)
                    for k, v in zip(keys_buf, values):
                        if v is not _empty:
                            yield k, v
                    keys_buf = []
            if keys_buf:
                values = await self.get_many(*keys_buf, default=_empty)
                for k, v in zip(keys_buf, values):
                    if v is not _empty:
                        yield k, v
            return

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

    async def get(self, key: Key, default: Value | None = None) -> Value:
        value = await self._client.get(key)
        return await self._transform_value(key, value, default)

    async def get_many(self, *keys: Key, default: Value | None = None) -> tuple[Value | None, ...]:
        if not keys:
            return ()

        if self._is_cluster:
            slot_map = self._group_keys_by_slot(keys)
            results = {}
            for _, subkeys in slot_map.items():
                values = await self._client.mget(*subkeys)
                results.update(dict(zip(subkeys, values)))

            return tuple(await asyncio.gather(*[self._transform_value(key, results[key], default) for key in keys]))

        values = await self._client.mget(*keys)
        if values is None:
            return tuple([default] * len(keys))
        return tuple(
            await asyncio.gather(*[self._transform_value(key, value, default) for key, value in zip(keys, values)])
        )

    def _group_keys_by_slot(self, keys: Iterable[Key]) -> dict[int, list[Key]]:
        groups: dict[int, list[Key]] = defaultdict(list)
        for k in keys:
            groups[self._client.keyslot(k)].append(k)  # type: ignore
        return groups

    def _group_pairs_by_slot(self, pairs: Mapping[Key, Value]) -> dict[int, dict[Key, Value]]:
        groups: dict[int, dict[Key, Value]] = defaultdict(dict)
        for k, v in pairs.items():
            groups[self._client.keyslot(k)][k] = v  # type: ignore
        return groups

    async def _transform_value(self, key: Key, value: bytes | None, default: Value | None):
        if value is None:
            return default
        if value.isdigit():
            return int(value)
        return await self._serializer.decode(self, key=key, value=value, default=default)

    async def incr(self, key: Key, value: int = 1, expire: float | None = None) -> int:
        if not expire:
            return await self._client.incr(key, amount=value)
        if self._sha.get("INCR_EXPIRE") is None:
            self._sha["INCR_EXPIRE"] = self._client.register_script(_INCR_EXPIRE.replace("\n", " "))  # type: ignore
        expire = expire or 0
        expire = int(expire * 1000)
        return await self._sha["INCR_EXPIRE"](keys=(key,), args=(value, expire))

    async def get_bits(self, key: Key, *indexes: int, size: int = 1) -> tuple[int, ...]:
        """
        https://redis.io/commands/bitfield
        """
        bitops = self._client.bitfield(key)  # type: ignore
        for index in indexes:
            bitops.get(fmt=f"u{size}", offset=f"#{index}")
        return tuple(await bitops.execute() or [])

    async def incr_bits(self, key: Key, *indexes: int, size: int = 1, by: int = 1) -> tuple[int, ...]:
        bitops = self._client.bitfield(key)  # type: ignore
        for index in indexes:
            bitops.incrby(fmt=f"u{size}", offset=f"#{index}", increment=by, overflow="SAT")
        return tuple(await bitops.execute())

    async def ping(self, message: bytes | None = None) -> bytes:
        await self._client.ping()  # type: ignore
        if message is None or message == b"PING":
            return b"PONG"
        return message

    async def set_raw(self, key: Key, value: Value, **kwargs: Any):
        return await self._client.set(key, value, **kwargs)

    async def get_raw(self, key: Key) -> Value:
        return await self._client.get(key)

    async def slice_incr(
        self,
        key: Key,
        start: int | float,
        end: int | float,
        maxvalue: int,
        expire: float | None = None,
    ) -> int:
        expire = expire or 0
        expire = int(expire * 1000)
        if self._sha.get("INCR_SLICE") is None:
            self._sha["INCR_SLICE"] = self._client.register_script(_INCR_SLICE.replace("\n", " "))  # type: ignore
        return await self._sha["INCR_SLICE"](keys=(key,), args=(start, end, maxvalue, expire))

    async def set_add(self, key: Key, *values: str, expire: float | None = None):
        if expire is None:
            return await self._client.sadd(key, *values)  # type: ignore
        expire = int(expire * 1000)

        if self._is_cluster:
            res = await self._client.sadd(key, *values)  # type: ignore
            await self._client.pexpire(key, expire)
            return res

        async with self._pipeline as pipe:
            await pipe.sadd(key, *values)
            await pipe.pexpire(key, expire)
            await pipe.execute()

    async def set_remove(self, key: Key, *values: str):
        await self._client.srem(key, *values)  # type: ignore

    async def set_pop(self, key: Key, count: int = 100) -> Iterable[str]:
        values = await self._client.spop(key, count)  # type: ignore
        if values is None:
            return []

        if isinstance(values, bytes):
            values = [values]

        return [value.decode() for value in values]

    async def get_keys_count(self) -> int:
        if not self._is_cluster:
            return await self._client.dbsize()
        primary_nodes = self._client.get_primaries()  # type: ignore
        sizes = await asyncio.gather(
            *(self._client.execute_command("DBSIZE", target_nodes=node) for node in primary_nodes)
        )
        return sum(sizes)

    async def close(self):
        if self.__is_init and self._client:
            await self._client.close()
            if not self._is_cluster:
                await self._client.connection_pool.disconnect()
        self.__is_init = False
