import asyncio

import pytest

from cashews.backends.memory import Memory

pytestmark = [pytest.mark.asyncio, pytest.mark.redis]


@pytest.fixture(name="create_cache")
def _create_cache(redis_dsn, backend_factory):
    from cashews.backends.client_side import BcastClientSide

    async def call(local_cache):
        return await backend_factory(BcastClientSide, redis_dsn, hash_key=None, local_cache=local_cache)

    return call


async def test_set_get_bcast(create_cache):
    cachef_local = Memory()
    cachef = await create_cache(cachef_local)
    caches_local = Memory()
    caches = await create_cache(caches_local)

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


async def test_set_none_bcast(create_cache):
    cachef_local = Memory()
    cachef = await create_cache(cachef_local)
    caches_local = Memory()
    caches = await create_cache(caches_local)

    await cachef.set("key", None, expire=0.1)
    await asyncio.sleep(0.01)  # skip init signal about invalidation
    assert await cachef_local.exists("key")
    assert await cachef.exists("key")

    assert await caches.get("key") is None
    assert await caches.exists("key")
    assert await caches_local.exists("key")


async def test_del_bcast(create_cache):
    cachef_local = Memory()
    cachef = await create_cache(cachef_local)
    caches_local = Memory()
    caches = await create_cache(caches_local)

    await cachef.set("key", b"value")
    await asyncio.sleep(0.05)  # skip init signal about invalidation

    assert await cachef.get("key") == b"value"
    assert await caches.get("key") == b"value"
    await cachef.delete("key")
    await asyncio.sleep(0.05)  # skip init signal about invalidation
    assert await caches.get("key") is None


async def test_rewrite_bcast(create_cache):
    cachef_local = Memory()
    cachef = await create_cache(cachef_local)
    caches_local = Memory()
    caches = await create_cache(caches_local)

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


async def test_simple_cmd_bcast(create_cache):
    local = Memory()
    cache = await create_cache(local)

    await cache.incr("key:2")
    assert await cache.get("key:2") == 1
    await cache.delete("key:2")
    assert await cache.get("key:2") is None
    assert await local.get("key:2") is None

    await cache.set("key:1", "test", 10)
    assert await cache.get("key:1") == "test"
    assert await local.get("key:1") == "test"
    await cache.expire("key:1", 100)
    assert await cache.get_expire("key:1") > 10
    assert await local.get_expire("key:1") > 10
    assert await cache.get("key:1") == "test"
    assert await local.get("key:1") == "test"

    await cache.clear()
    cache.close()


async def test_simple_cmd_bcast_many(create_cache):
    local = Memory()
    cache = await create_cache(local)
    await cache.set("key:1", "test")
    assert await cache.get("key:1") == "test"
    assert await local.get("key:1") == "test"

    assert await cache.get_many("key:1", "key:2") == ("test", None)

    async for key in cache.scan("key:*"):
        assert key == "key:1"
        break
    else:
        assert False

    async for key, value in cache.get_match("key:*"):
        assert key == "key:1"
        assert value == "test"
        break
    else:
        assert False

    await local.clear()

    assert await cache.get_many("key:1", "key:2") == ("test", None)

    async for key in cache.scan("key:*"):
        assert key == "key:1"

    async for key, value in cache.get_match("key:*"):
        assert key == "key:1"
        assert value == "test"

    assert await local.get("key:1") == "test"

    assert await cache.delete_match("key:*")
    assert await cache.get("key:1") is None
    assert await local.get("key:1") is None

    async for _ in cache.scan("key:*"):
        assert False

    async for _ in cache.get_match("key:*"):
        assert False
