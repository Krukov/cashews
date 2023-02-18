"""
Client side cache is hybrid of mem and redis cache
in theory local cache should be consistence
GET:
-> IN mem cache -> Y -> return
                -> N -> in redis cache -> Y -> store in mem cache -> return
                                       -> N -> compete -> store in mem and in redis -> notify others by channel to invalidate  # noqa: E501

INVALIDATE:

    Redis client side cache cons:
        - if client set he didnt receive message (so we can't update local cache on set without get on redis)
        - message only for first set (2 and after will miss) (solve by request resource after message
        - no control
        - redis >= 6
    Redis client side cache pros:
        + mem cache without ttl
        + no trash

Redis client side caching with non broadcast option weed, with pool of connections it is hard to process connection
lifetime and subscribe for get requests also if we set some value with ttl every client who get value from redis can
also request a ttl and store in local cache with ttl but we steal should know if someone overwrite value or delete it
Broadcasting mode is more useful as we can subscribe for all keys with prefix and invalidate key

https://engineering.redislabs.com/posts/redis-assisted-client-side-caching-in-python/
https://redis.io/topics/client-side-caching
"""

import asyncio
import logging
from typing import Any, AsyncIterator, Mapping, Optional, Tuple

from redis.exceptions import ConnectionError as RedisConnectionError

from cashews._typing import Key, Value
from cashews.backends.memory import Memory

from . import Redis

_REDIS_INVALIDATE_CHAN = "__redis__:invalidate"
_empty = object()
_empty_in_redis = object()  # set when we know that key not in redis
_RECONNECT_WAIT = 10
_DEFAULT_PREFIX = "cashews:"
BCAST_ON = "CLIENT TRACKING on REDIRECT {client_id} BCAST PREFIX {prefix}"
logger = logging.getLogger(__name__)


class BcastClientSide(Redis):
    """
    Cache backend with redis as main storage and client side mem storage that invalidated by
    redis channel for client-side-caching.

    Subscribe with broadcasting by prefix for invalidate by redis>=6
    https://redis.io/topics/client-side-caching
    """

    def __init__(self, *args: Any, local_cache=None, client_side_prefix: str = _DEFAULT_PREFIX, **kwargs: Any) -> None:
        self._local_cache = Memory(size=10000) if local_cache is None else local_cache
        self._prefix = client_side_prefix
        self._recently_update = Memory(size=500, check_interval=60)
        self._listen_task = None
        self._expire_for_recently_update = 5
        self._listen_started = asyncio.Event()
        self.__listen_stop = asyncio.Event()
        super().__init__(*args, **kwargs)

    async def init(self):
        self._listen_started = asyncio.Event()
        self.__listen_stop = asyncio.Event()
        await self._local_cache.init()
        await self._recently_update.init()
        await super().init()
        self.__is_init = False
        self._listen_task = asyncio.create_task(self._listen_invalidate_forever())
        try:
            await asyncio.wait_for(self._listen_started.wait(), timeout=self._kwargs["socket_timeout"])
        except (TimeoutError, asyncio.TimeoutError):
            if self._listen_task.done():
                raise self._listen_task.exception()
            self._listen_task.cancel()
            raise
        self.__is_init = True

    async def _mark_as_recently_updated(self, key: Key):
        await self._recently_update.set(key, True, expire=self._expire_for_recently_update)

    def _remove_prefix(self, key: Key) -> Key:
        return key[len(self._prefix) :]

    def _add_prefix(self, key: Key) -> Key:
        return self._prefix + key

    async def _listen_invalidate_forever(self):
        while not self.__listen_stop.is_set():
            try:
                await self._listen_invalidate()
            except (RedisConnectionError, ConnectionRefusedError):
                logger.error("broken connection with redis. Clearing client side storage")
                self._listen_started.clear()
                await self._local_cache.clear()
                await asyncio.sleep(_RECONNECT_WAIT)
            except Exception:
                self._listen_started.clear()
                await self._local_cache.clear()
                raise

    async def _get_channel(self):
        pubsub = self._client.pubsub()
        await pubsub.execute_command(b"CLIENT", b"ID")
        client_id = await pubsub.parse_response()
        await pubsub.execute_command(*BCAST_ON.format(client_id=client_id, prefix=self._prefix).encode().split())
        await pubsub.parse_response()
        await pubsub.subscribe(_REDIS_INVALIDATE_CHAN)
        return pubsub

    async def _listen_invalidate(self):
        channel = await self._get_channel()
        self._listen_started.set()
        await self._local_cache.clear()
        while not self.__listen_stop.is_set():
            message = await channel.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if message is None or "data" not in message:
                continue
            if message["data"] is None:  # flushdb
                logger.debug("flush: clear local cache")
                await self._local_cache.clear()
                continue
            for key in message["data"]:
                original_key = key.decode()
                key = self._remove_prefix(original_key)
                if not await self._recently_update.get(key):
                    logger.debug("invalidate the key %s", key)
                    await self._local_cache.delete(key)
                    await self._call_on_remove_callbacks(original_key)
                else:
                    logger.debug("the key `%s`: recently update", key)
                    await self._recently_update.delete(key)

    async def get(self, key: Key, default: Optional[Value] = None) -> Value:
        if self._listen_started.is_set():
            value = await self._local_cache.get(key, default=_empty)
            if value is _empty_in_redis:
                return default
            if value is not _empty:
                return value
        value = await super().get(self._add_prefix(key), default=_empty)
        if value is not _empty:
            await self._local_cache.set(key, value)
            return value
        await self._local_cache.set(key, _empty_in_redis)
        return default

    async def set(
        self,
        key: Key,
        value: Value,
        expire: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        await self._local_cache.set(key, value, expire, exist)
        await self._mark_as_recently_updated(key)
        _set = await super().set(self._add_prefix(key), value, expire, exist)
        if _set:
            await self._local_cache.set(key, value, expire)
        else:
            await self._recently_update.delete(key)
        return _set

    async def set_many(self, pairs: Mapping[Key, Value], expire: Optional[float] = None):
        await self._local_cache.set_many(pairs, expire)
        for key in pairs.keys():
            await self._mark_as_recently_updated(key)
        return await super().set_many(
            {self._add_prefix(key): value for key, value in pairs.items()},
            expire=expire,
        )

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Key]:  # type: ignore
        async for key in super().scan(self._add_prefix(pattern), batch_size=batch_size):
            yield self._remove_prefix(key)

    async def get_many(self, *keys: Key, default: Optional[Value] = None) -> Tuple[Optional[Value], ...]:
        missed_keys = {self._add_prefix(key) for key in keys}
        values = {key: default for key in keys}
        if self._listen_started.is_set():
            for i, value in enumerate(await self._local_cache.get_many(*keys, default=_empty)):
                key = keys[i]
                if value is _empty:
                    continue
                if value is _empty_in_redis:
                    value = default
                values[key] = value
                missed_keys.remove(self._add_prefix(key))
        missed_values = await super().get_many(*missed_keys, default=default)
        missed = dict(zip((self._remove_prefix(key) for key in missed_keys), missed_values))
        for key, value in missed.items():
            if value is not default:
                await self._local_cache.set(key, value)
            else:
                await self._local_cache.set(key, _empty_in_redis)
        return tuple(missed.get(key, value) for key, value in values.items())

    async def get_match(self, pattern: str, batch_size: int = 100) -> AsyncIterator[Tuple[Key, Value]]:  # type: ignore
        cursor = 0
        while True:
            cursor, keys = await self._client.scan(cursor, match=self._add_prefix(pattern), count=batch_size)
            if not keys:
                if not cursor:
                    return
                continue
            keys = [self._remove_prefix(key.decode()) for key in keys]
            values = await self.get_many(*keys, default=_empty)
            for key, value in zip(keys, values):
                if value is not _empty:
                    yield key, value
            if not cursor:
                return

    async def incr(self, key: Key, value: int = 1, expire: Optional[float] = None) -> int:
        _value = await super().incr(self._add_prefix(key), value=value, expire=expire)
        if _value:
            await self._local_cache.set(key, _value, expire=expire)
            await self._mark_as_recently_updated(key)
        return _value

    async def delete(self, key: Key) -> bool:
        await self._local_cache.set(key, _empty_in_redis)
        return await super().delete(self._add_prefix(key))

    async def delete_many(self, *keys: Key):
        _keys = []
        for key in keys:
            _keys.append(self._add_prefix(key))
            await self._local_cache.set(key, _empty_in_redis)
        return await super().delete_many(*_keys)

    async def delete_match(self, pattern: str):
        await self._local_cache.delete_match(pattern)
        return await super().delete_match(self._add_prefix(pattern))

    async def expire(self, key: Key, timeout: float):
        local_value = await self._local_cache.get(key, default=_empty)
        if local_value not in (_empty, _empty_in_redis):
            await self._local_cache.expire(key, timeout)
            await self._mark_as_recently_updated(key)
        result = await super().expire(self._add_prefix(key), timeout)
        return result

    async def get_expire(self, key: Key) -> int:
        if await self._local_cache.get_expire(key) > 0:
            if self._listen_started.is_set():
                return await self._local_cache.get_expire(key)
        expire = await super().get_expire(self._add_prefix(key))
        await self._local_cache.expire(key, expire)
        return expire

    async def exists(self, key: Key) -> bool:
        if self._listen_started.is_set():
            local_value = await self._local_cache.get(key, default=_empty)
            if local_value not in (_empty, _empty_in_redis):
                return True
        return await super().exists(self._add_prefix(key))

    async def set_lock(self, key: Key, value: Value, expire: float) -> bool:
        await self._mark_as_recently_updated(key)
        await self._local_cache.set_lock(key, value, expire)
        return await super().set_lock(self._add_prefix(key), value, expire=expire)

    async def unlock(self, key: Key, value: Value) -> bool:
        await self._local_cache.unlock(key, value)
        return await super().unlock(self._add_prefix(key), value)

    async def get_size(self, key: Key) -> int:
        return await super().get_size(self._add_prefix(key))

    async def clear(self):
        await self._local_cache.clear()
        return await super().clear()

    async def close(self):
        self.__listen_stop.set()
        if self._listen_task is not None:
            await self._listen_task
            self._listen_task = None
        await self._local_cache.close()
        await self._recently_update.close()
        await super().close()

    async def _call_on_remove_callbacks(self, *keys: Key):
        keys = [self._remove_prefix(key) for key in keys]
        await super()._call_on_remove_callbacks(*keys)
