"""
Client side cache is hybrid of mem and redis cache
in theory local cache should be consinstace
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

Redis client side caching with non broudcast option look like unusefull, with pool of connections it is hard to process connection livetime and subscribe for get requests
also if we set some value with ttl every client who get value from redis can also request a ttl and store in local cache with ttl
but we steal shoud know if someone overwrite value or delite it
Broadcasting mode is more usefull as we can subcribe for all keys with prefix and invalidate key

https://engineering.redislabs.com/posts/redis-assisted-client-side-caching-in-python/
https://redis.io/topics/client-side-caching


problem with own pubsub with new values  - big size of of messages
                                         + good for small number of keys
                                         + fast update without request
                                         + warmup cache on restart
                                         + take only what requested
"""

import asyncio
import datetime
import json
import pickle
import uuid

import aioredis

from .memory import MemoryInterval
from .redis import Redis

_OWN_CHAN = "_cashews:client:side"
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
        self._local_cache = MemoryInterval() if local_cache is None else local_cache
        self._prefix = prefix
        self.__listen_task = None
        super().__init__(*args, **kwargs)

    async def init(self):
        await self._local_cache.init()
        await super().init()
        self.__listen_task = asyncio.create_task(self._listen_invalidate_forever())

    async def _listen_invalidate_forever(self):
        while True:
            await self._local_cache.clear()
            try:
                await self._listen_invalidate()
            except (aioredis.errors.ConnectionClosedError, ConnectionRefusedError):
                await self._local_cache.clear()
                await asyncio.sleep(_RECONNECT_WAIT)
            finally:
                await self._local_cache.clear()

    async def _get_channel(self) -> aioredis.Channel:
        conn = aioredis.Redis(await self.connection.acquire())
        client_id = await conn.execute(b"CLIENT", b"ID")
        await conn.execute(*BCAST_ON.format(client_id=client_id, prefix=self._prefix).encode().split())
        channel, *_ = await conn.subscribe(_REDIS_INVALIDATE_CHAN)
        return channel

    async def _listen_invalidate(self):
        channel = await self._get_channel()
        while await channel.wait_message():
            key, *_ = await channel.get()
            if key == b"\x00":
                continue
            key = key.decode().replace(self._prefix, "")
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
        await self._local_cache.set(key, value, *args, **kwargs)
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

    async def expire(self, key, timeout):
        await self._local_cache.expire(key, timeout)
        return await super().expire(self._prefix + key, timeout)

    async def get_expire(self, key: str) -> int:
        if await self._local_cache.get_expire(key) != -1:
            return await self._local_cache.get_expire(key)
        return await super().get_expire(self._prefix + key)

    async def clear(self):
        await self._local_cache.clear()
        return await super().clear()

    async def close(self):
        if self.__listen_task is not None:
            self.__listen_task.cancel()
        await super().close()


class UpdateChannelClientSide(Redis):
    """
    Cache backend with redis as main storage and client side mem storage that invalidated by
    own redis channel for update cache.

    when value delete or set backend publish this event (if set - with new value, if expire with ttl)
    """

    def __init__(self, *args, local_cache=None, **kwargs):
        self._local_cache = MemoryInterval() if local_cache is None else local_cache
        self._publish_queue = asyncio.Queue()
        self.__virtual_client_id = str(uuid.uuid4())
        self.__tasks = []
        super().__init__(*args, **kwargs)

    async def init(self):
        await self._local_cache.init()
        await super().init()
        conn = aioredis.Redis(await self.connection.acquire())
        channel, *_ = await conn.subscribe(_OWN_CHAN)
        self.__tasks.append(asyncio.create_task(self._listen_invalidate(channel)))
        self.__tasks.append(asyncio.create_task(self._publish_worker()))
        for task in self.__tasks:
            task.add_done_callback(lambda *_, **__: print(_, __))

    async def _listen_invalidate(self, channel: aioredis.Channel):
        while await channel.wait_message():
            message = await channel.get()
            message = json.loads(message)
            if message["source"] == self.__virtual_client_id:
                continue
            await self._process_key_event(message["key"], event=message["event"], data=message["data"])

    async def _process_key_event(self, key, event, data):
        if event == "del":
            return await self._local_cache.delete(key)
        if event == "set":
            value = pickle.loads(data.encode("latin1"))
            return await self._local_cache.set(key, value, expire=30)
            # we set expire 30 because we expect message about exp next after set
        if event == "exp":
            return self._local_cache._set_expire_at(key, datetime.datetime.fromtimestamp(data))

    async def _publish_worker(self):
        conn = aioredis.Redis(await self.connection.acquire())
        while True:
            message = await self._publish_queue.get()
            message = json.dumps(message)
            await conn.publish(_OWN_CHAN, message)
            self._publish_queue.task_done()

    async def get(self, key: str, default=None):
        value = await self._local_cache.get(key, default=_empty)
        if value is not _empty:
            return value
        value = await super().get(key, default=_empty)
        if value is not _empty:
            await self._local_cache.set(key, value)
            return value
        return default

    async def set(self, key: str, value, *args, expire=None, **kwargs):
        await self._local_cache.set(key, value, *args, expire=expire, **kwargs)
        self._publish_key_event(key, "set", value)
        if expire:
            self._publish_key_event(
                key, "exp", (datetime.datetime.utcnow() + datetime.timedelta(seconds=expire)).timestamp()
            )
        return await super().set(key, value, *args, expire=expire, **kwargs)

    async def delete(self, key: str):
        await self._local_cache.delete(key)
        self._publish_key_event(key, "del", None)
        return await super().delete(key=key)

    def _publish_key_event(self, key, event, data):
        if event == "set":
            data = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL, fix_imports=False).decode("latin1")
        self._publish_queue.put_nowait(
            {"event": event, "data": data, "key": key, "id": str(uuid.uuid4()), "source": self.__virtual_client_id}
        )

    async def close(self):
        await self._publish_queue.put({"source": self.__virtual_client_id, "event": "go away"})
        for task in self.__tasks:
            task.cancel()
        await super().close()
