import os

import pytest

from cashews.key import register_template

pytestmark = [pytest.mark.asyncio, pytest.mark.redis]


@pytest.fixture(name="cache")
async def _cache(redis_dsn):
    from cashews.backends.index import IndexRedis

    redis = IndexRedis(address=redis_dsn, hash_key=None, index_field="user", index_name="test")
    await redis.init()
    await redis.clear()
    return redis


async def test_set(cache):
    register_template(test_set, "key:{user}:{account}")
    await cache.set("key:jon:10", b"val")
    assert await cache.hget("test:jon", "key:10") == b"val"


async def test_get(cache):
    register_template(test_get, "key:{user}:{account}")
    await cache.hset("test:jon", "key:10", b"val")

    assert await cache.get("key:jon:10") == b"val"


async def test_get_set_no_template(cache):
    await cache.set("1:jon:10", b"val")
    assert await cache.get("1:jon:10") == b"val"


async def test_delete_match(cache):
    register_template(test_get, "key:{user}:{account}")
    await cache.hset("test:jon", "key:10", b"val")

    await cache.delete_match("key:jon:*")
    assert await cache.get("key:jon:10") is None


async def test_delete(cache):
    register_template(test_get, "key:{user}:{account}")
    await cache.hset("test:jon", "key:10", b"val")

    await cache.delete("key:jon:10")
    assert await cache.get("key:jon:10") is None
