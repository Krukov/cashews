import asyncio
from unittest.mock import Mock

import pytest

from cashews.backends.memory import Memory
from cashews.wrapper import Cache

pytestmark = pytest.mark.asyncio


@pytest.fixture(name="target")
def _target():
    return Mock(wraps=Memory())


@pytest.fixture(name="cache")
def _cache(target):
    cache = Cache()
    cache._add_backend(target)
    return cache


async def test_disable_cmd(cache):
    await cache.init("mem://localhost")
    cache.disable("incr")
    await cache.set("test", 10)
    await cache.incr("test")
    assert await cache.get("test") == 10

    cache.enable("incr")
    await cache.incr("test")
    assert await cache.get("test") == 11


async def test_disable_ctz(cache):
    await cache.init("mem://localhost")
    cache.enable()

    async def test():
        await cache.set("test", "1")
        assert await cache.get("test") == "1"
        cache.disable("set")
        await cache.set("test", "2")

    await asyncio.create_task(test())
    assert await cache.get("test") == "1"
    await cache.set("test", "3")
    assert await cache.get("test") == "3"


async def test_disable_decorators(cache: Cache, target):
    cache.disable()
    data = (i for i in range(10))

    @cache(ttl=1)
    @cache.soft(ttl=1)
    @cache.failover(ttl=1)
    @cache.hit(ttl=1, cache_hits=1)
    @cache.circuit_breaker(ttl=1, errors_rate=1, period=1)
    @cache.rate_limit(ttl=1, limit=1, period=1)
    @cache.early(ttl=1)
    @cache.dynamic()
    @cache.locked(ttl=1)
    async def func():
        return next(data)

    assert await func() == 0
    assert await func() == 1
    target.get.assert_not_called()
    target.set.assert_not_called()

    cache.enable()
    assert await func() == 2
    assert await func() == 2


async def test_disable_bloom(cache: Cache, target: Mock):
    cache.disable()

    @cache.bloom(index_size=10, number_of_hashes=1)
    async def func():
        return True

    await func.set()
    assert await func()
    target.incr_bits.assert_not_called()
    target.get_bits.assert_not_called()

    cache.enable()
    await func.set()
    assert await func()
    target.incr_bits.assert_called()
    target.get_bits.assert_called()


async def test_disable_decorators_get(cache: Cache):
    data = (i for i in range(10))
    await cache.init("mem://localhost")

    @cache(ttl=1)
    async def func():
        return next(data)

    assert cache.is_enable()
    assert cache.is_enable("set", prefix="cache")
    assert cache.is_enable("get", prefix="cache")
    assert cache.is_enable("set", prefix="")
    assert await func() == 0
    assert await func() == 0

    cache.disable("get")

    assert not cache.is_enable("get")
    assert cache.is_enable("set")

    assert await func() == 1
    assert await func() == 2

    cache.enable("get")
    assert await func() == 2


async def test_disable_decorators_set(cache: Cache):
    data = (i for i in range(10))
    cache.disable("set")

    @cache(ttl=1)
    async def func():
        return next(data)

    assert await func() == 0
    assert await func() == 1

    cache.enable("set")
    assert await func() == 2
    assert await func() == 2
