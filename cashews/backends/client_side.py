"""
Client side cache is hybrid of mem and redis cache
in theory local cache should be consistence
GET:
-> IN mem cache -> Y -> return
                -> N -> in redis cache -> Y -> store in mem cache -> return
                                       -> N -> compete -> store in mem and in redis -> notify others by channel to invalidete

INVALIDATE:

problem with redis client side cache  - if client set he didnt receive message (so we can't update local cache on set without get on redis)
                                        - message only for first set (2 and after will miss) (solve by request resource after message
                                        - no control
                                        - redis >= 6
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

import aioredis

from .memory import Memory
from .redis import Redis

_REDIS_INVALIDATE_CHAN = "__redis__:invalidate"
_empty = object()
_RECONNECT_WAIT = 10
_DEFAULT_PREFIX = "cashews:"
BCAST_ON = "CLIENT TRACKING on REDIRECT {client_id} BCAST PREFIX {prefix} NOLOOP"


class BcastClientSide(Redis):
    """
    Cache backend with redis as main storage and client side mem storage that invalidated by
    redis channel for client-side-caching.

    Subscribe with broadcasting by prefix for invalidate by redis>=6
    https://redis.io/topics/client-side-caching
    """

    def __init__(self, *args, local_cache=None, prefix=_DEFAULT_PREFIX, **kwargs):
        self._local_cache = Memory() if local_cache is None else local_cache
        self._prefix = prefix
        self.__listen_task = None
        super().__init__(*args, **kwargs)

    async def init(self):
        await self._local_cache.init()
        await super().init()
        self.__listen_task = asyncio.create_task(self._listen_invalidate_forever())

    async def _listen_invalidate_forever(self):
        while True:
            try:
                await self._listen_invalidate()
            except (aioredis.errors.ConnectionClosedError, ConnectionRefusedError):
                await self._local_cache.clear()
                await asyncio.sleep(_RECONNECT_WAIT)

    async def _get_channel(self) -> aioredis.Channel:
        conn = aioredis.Redis(await self.connection.acquire())
        client_id = await conn.execute(b"CLIENT", b"ID")
        await conn.execute(*BCAST_ON.format(client_id=client_id, prefix=self._prefix).encode().split())
        channel, *_ = await conn.subscribe(_REDIS_INVALIDATE_CHAN)
        return channel

    async def _listen_invalidate(self):
        channel = await self._get_channel()
        while await channel.wait_message():
            message = await channel.get()
            if message is None:
                continue
            key, *_ = message
            if key == b"\x00":
                continue
            key = key.decode().lstrip(self._prefix)
            await self._local_cache.delete(key)

    async def get(self, key: str, default=None):
        value = await self._local_cache.get(key, default=_empty)
        if value is not _empty:
            return value
        value = await super().get(self._prefix + key, default=_empty)
        if value is not _empty:
            await self._local_cache.set(key, value)
            return value
        return default

    async def set(self, key: str, value, *args, **kwargs):
        if await self._local_cache.get(key) == value:
            # If value in current client_cache - skip resetting
            return 0
        await self._local_cache.set(key, value, *args, **kwargs)  # not async by the way
        return await super().set(self._prefix + key, value, *args, **kwargs)

    async def get_many(self, *keys):
        values = await self._local_cache.get_many(*keys)
        missed_keys = [key for key, value in zip(keys, values) if value is not None]
        missed_values = await super().get_many(*missed_keys)
        missed = dict(zip(missed_keys, missed_values))
        return [missed.get(key, value) for key, value in zip(keys, values)]

    def incr(self, key):
        return super().incr(self._prefix + key)

    async def delete(self, key: str):
        await self._local_cache.delete(key)
        return await super().delete(self._prefix + key)

    async def delete_match(self, pattern: str):
        await self._local_cache.delete_match(pattern)
        return await super().delete_match(self._prefix + pattern)

    async def expire(self, key, timeout):
        # `expire` sends message to invalidate channel. This results in deleting key 
        # from local_cache. To avoid this we first capture original value, and then
        # set it again with appropriate `timeout`.
        local_value = await self._local_cache.get(key, default=_empty)
        result = await super().expire(self._prefix + key, timeout)
        if local_value is not _empty:
            await self._local_cache.delete(key)
            await self._local_cache.set(key, local_value, timeout)
        return result

    async def get_expire(self, key: str) -> int:
        if await self._local_cache.get_expire(key) != -1:
            return await self._local_cache.get_expire(key)
        expire = await super().get_expire(self._prefix + key)
        await self._local_cache.expire(key, expire)
        return expire

    async def exists(self, key) -> bool:
        return await self._local_cache.exists(key) or await super().exists(self._prefix + key)

    async def clear(self):
        await self._local_cache.clear()
        return await super().clear()

    def close(self):
        if self.__listen_task is not None:
            self.__listen_task.cancel()
        super().close()
