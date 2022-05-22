from unittest.mock import Mock

import pytest

from cashews.wrapper import Cache

pytestmark = [pytest.mark.asyncio, pytest.mark.redis]


@pytest.fixture
def redis_backend():
    from cashews.backends.redis import Redis

    return Redis


async def test_safe_redis(redis_backend):
    redis = redis_backend(safe=True, address="redis://localhost:9223", hash_key=None)
    await redis.init()
    assert await redis.set("test", "test") is False

    assert await redis.set_lock("test", "test", 1) is False
    assert await redis.unlock("test", "test") is None
    assert await redis.is_locked("test") is False

    assert await redis.get("test", default="no") == "no"
    assert await redis.get("test") is None
    assert await redis.get_many("test", "test2") == (None, None)

    assert await redis.get_expire("test") == 0
    assert await redis.incr("test") is None
    assert await redis.get_size("test") == 0
    async for i in redis.scan("*"):
        assert False

    async for k, v in redis.get_match("*"):
        assert False

    assert await redis.delete("test") == 0


async def test_cache_decorators_on_redis_down(redis_backend):
    mock = Mock(return_value="val")
    cache = Cache()
    cache._add_backend(redis_backend, safe=True, address="redis://localhost:9223", hash_key=None)

    @cache(ttl=1)
    @cache.failover(1)
    @cache.hit(ttl=1, cache_hits=1)
    @cache.circuit_breaker(ttl=1, errors_rate=1, period=1)
    @cache.rate_limit(ttl=1, limit=1, period=1)
    @cache.early(ttl=1)
    @cache.dynamic()
    @cache.locked(ttl=1)
    async def func():
        return mock()

    assert await func() == "val"
    assert mock.call_count == 1

    assert await func() == "val"
    assert mock.call_count == 2
