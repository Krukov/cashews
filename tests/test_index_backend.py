import os

import pytest
from cashews.backends.index import IndexRedis
from cashews.key import register_template

pytestmark = pytest.mark.asyncio
REDIS_TESTS = bool(os.environ.get("USE_REDIS"))


@pytest.fixture(name="cache")
async def _cache():
    redis = IndexRedis(address="redis://", hash_key=None, index_field="user", index_name="test")
    await redis.init()
    await redis.clear()
    return redis


@pytest.mark.skipif(not REDIS_TESTS, reason="only for redis")
async def test_set(cache):
    register_template(test_set, "key:{user}:{account}")
    await cache.set("key:jon:10", b"val")
    assert await cache.hget("test:jon", "key:10") == b"val"


@pytest.mark.skipif(not REDIS_TESTS, reason="only for redis")
async def test_get(cache):
    register_template(test_get, "key:{user}:{account}")
    await cache.hset("test:jon", "key:10", b"val")

    assert await cache.get("key:jon:10") == b"val"


@pytest.mark.skipif(not REDIS_TESTS, reason="only for redis")
async def test_get_set_no_template(cache):
    await cache.set("1:jon:10", b"val")
    assert await cache.get("1:jon:10") == b"val"


@pytest.mark.skipif(not REDIS_TESTS, reason="only for redis")
async def test_delete_match(cache):
    register_template(test_get, "key:{user}:{account}")
    await cache.hset("test:jon", "key:10", b"val")

    await cache.delete_match("key:jon:*")
    assert await cache.get("key:jon:10") is None


@pytest.mark.skipif(not REDIS_TESTS, reason="only for redis")
async def test_delete(cache):
    register_template(test_get, "key:{user}:{account}")
    await cache.hset("test:jon", "key:10", b"val")

    await cache.delete("key:jon:10")
    assert await cache.get("key:jon:10") is None
