import asyncio
import os

import pytest
from cashews.backends.client_side import BcastClientSide
from cashews.backends.memory import Memory

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
    await asyncio.sleep(0.01)  # skip init signal about invalidation
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

    await asyncio.sleep(0.15)
    assert await caches.get("key") is None
    assert await cachef.get("key") is None


@pytest.mark.skipif(not REDIS_TESTS, reason="only for redis")
async def test_simple_cmd_bcast(create_cache):
    local = Memory()
    cache = await create_cache(BcastClientSide, local)

    await cache.set("key:1", "test", 1)
    await asyncio.sleep(0.05)  # skip init signal about invalidation
    assert await cache.get("key:1") == "test"

    await cache.incr("key:2")
    assert await cache.get("key:2") == 1
    await cache.delete("key:2")
    assert await cache.get("key:2") is None
    assert await local.get("key:2") is None

    assert await cache.get("key:1") == "test"
    await cache.expire("key:1", 1)
    assert await cache.get_expire("key:1") > 0
    assert await local.get_expire("key:1") > 0

    assert await cache.delete_match("key:*")
    assert await cache.get("key:1") is None
    assert await local.get("key:1") is None
    await cache.clear()
    cache.close()
