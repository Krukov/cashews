import asyncio
import os

import pytest
from cashews.backends.client_side import BcastClientSide, UpdateChannelClientSide
from cashews.backends.memory import Memory, MemoryInterval

pytestmark = pytest.mark.asyncio
REDIS_TESTS = bool(os.environ.get("USE_REDIS"))


@pytest.fixture(name="create_cache")
def _create_cache():
    async def call(cache_class, local_cache=Memory()):
        redis = cache_class("redis://", hash_key=None, local_cache=local_cache)
        await redis.init()
        await redis.clear()
        return redis

    return call


@pytest.mark.skipif(not REDIS_TESTS, reason="only for redis")
async def test_set_get_bcast(create_cache):
    cachef_local = Memory()
    cachef = await create_cache(BcastClientSide, cachef_local)
    caches_local = Memory()
    caches = await create_cache(BcastClientSide, caches_local)

    await cachef.set("key", b"value", expire=0.1)
    await asyncio.sleep(0.05)  # skip init signal about invalidation
    assert await cachef.get("key") == b"value"
    assert await caches.get("key") == b"value"
    assert await cachef_local.get("key") == b"value"
    assert await caches_local.get("key") == b"value"
    await asyncio.sleep(0.2)
    assert await cachef_local.get("key") is None
    assert await caches_local.get("key") is None

    assert await caches.get("key") is None


@pytest.mark.skipif(not REDIS_TESTS, reason="only for redis")
async def test_del_bcast(create_cache):
    cachef_local = Memory()
    cachef = await create_cache(BcastClientSide, cachef_local)
    caches_local = Memory()
    caches = await create_cache(BcastClientSide, caches_local)

    await cachef.set("key", b"value")
    await asyncio.sleep(0.05)  # skip init signal about invalidation

    assert await cachef.get("key") == b"value"
    assert await caches.get("key") == b"value"
    await cachef.delete("key")
    await asyncio.sleep(0.05)  # skip init signal about invalidation
    assert await caches.get("key") is None


@pytest.mark.skipif(not REDIS_TESTS, reason="only for redis")
async def test_rewrite_bcast(create_cache):
    cachef_local = Memory()
    cachef = await create_cache(BcastClientSide, cachef_local)
    caches_local = Memory()
    caches = await create_cache(BcastClientSide, caches_local)

    await cachef.set("key", b"value")
    await asyncio.sleep(0.05)  # skip init signal about invalidation

    assert await cachef.get("key") == b"value"
    assert await caches.get("key") == b"value"

    await caches.set("key", b"new", expire=0.1)
    await asyncio.sleep(0.05)  # skip init signal about invalidation

    assert await cachef.get("key") == b"new"

    await asyncio.sleep(0.06)
    assert await caches.get("key") is None

    await asyncio.sleep(0)
    assert await cachef.get("key") is None


@pytest.mark.skipif(not REDIS_TESTS, reason="only for redis")
async def test_set_get_custom_chan(create_cache):
    cachef_local = Memory()
    cachef = await create_cache(UpdateChannelClientSide, cachef_local)
    caches_local = MemoryInterval()
    caches = await create_cache(UpdateChannelClientSide, caches_local)
    caches._pool_or_conn.get = None

    await cachef.set("key", b"value", expire=0.1)
    assert await cachef_local.get("key") == b"value"
    await asyncio.sleep(0.05)  #  wait cache update
    assert await cachef_local.get("key") == b"value"
    assert await caches_local.get("key") == b"value"
    assert await caches_local.get_expire("key") < 0.05
    assert await caches_local.get_expire("key") > 0.03
    assert await cachef.get("key") == b"value"
    assert await caches.get("key") == b"value"
    assert await cachef_local.get("key") == b"value"
    await asyncio.sleep(0.2)

    assert await cachef_local.get("key") is None
    assert await caches_local.get("key") is None

    assert await caches.get("key") is None


@pytest.mark.skipif(not REDIS_TESTS, reason="only for redis")
async def test_set_get_del_custom_chan_serialize(create_cache):
    cachef_local = Memory()
    cachef = await create_cache(UpdateChannelClientSide, cachef_local)
    caches_local = MemoryInterval()
    caches = await create_cache(UpdateChannelClientSide, caches_local)
    value = {"my": None}
    await cachef.set("key", value)
    await asyncio.sleep(0.05)  # wait cache update
    assert await cachef_local.get("key") == value
    assert await caches_local.get("key") == value
    assert await caches.get("key") == value

    await cachef.delete("key")
    assert await cachef_local.get("key") is None

    await asyncio.sleep(0.05)  # wait cache update
    assert await caches_local.get("key") is None
