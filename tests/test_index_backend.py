import pytest

from cashews.formatter import register_template

pytestmark = [pytest.mark.asyncio, pytest.mark.redis]


@pytest.fixture(name="cache")
async def _cache(redis_dsn, backend_factory):
    from cashews.backends.index import IndexRedis

    return await backend_factory(
        IndexRedis,
        address=redis_dsn,
        hash_key=None,
        index_field="user",
        index_name="test",
    )


async def test_set(cache):
    register_template(test_set, "key:{user}:{account}")
    await cache.set("key:jon:10", b"val")
    assert await cache._client.hget("test:jon", "key:10") == b"val"


async def test_get(cache):
    register_template(test_get, "key:{user}:{account}")
    await cache._client.hset("test:jon", "key:10", b"val")

    assert await cache.get("key:jon:10") == b"val"


async def test_get_set_no_template(cache):
    await cache.set("1:jon:10", b"val")
    assert await cache.get("1:jon:10") == b"val"


async def test_delete_match(cache):
    register_template(test_get, "key:{user}:{account}")
    await cache._client.hset("test:jon", "key:10", b"val")

    await cache.delete_match("key:jon:*")
    assert await cache.get("key:jon:10") is None


async def test_delete(cache):
    register_template(test_get, "key:{user}:{account}")
    await cache._client.hset("test:jon", "key:10", b"val")

    await cache.delete("key:jon:10")
    assert await cache.get("key:jon:10") is None
