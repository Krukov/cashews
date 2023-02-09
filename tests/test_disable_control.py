import asyncio
from random import random
from unittest.mock import Mock

import pytest

from cashews.commands import Command
from cashews.wrapper import Cache

pytestmark = pytest.mark.asyncio


async def test_disable_cmd(cache):
    cache.disable(Command.INCR)
    await cache.set("test", 10)
    await cache.incr("test")
    assert await cache.get("test") == 10

    cache.enable(Command.INCR)
    await cache.incr("test")
    assert await cache.get("test") == 11


async def test_is_disable():
    cache = Cache()
    await cache.init("mem://localhost")

    assert not cache.is_disable()
    assert not cache.is_disable(Command.GET)
    assert not cache.is_full_disable

    assert cache.is_enable()
    assert cache.is_enable(Command.GET)

    cache.disable(Command.GET)

    assert cache.is_disable()
    assert cache.is_disable(Command.GET)
    assert not cache.is_disable(Command.SET)
    assert not cache.is_full_disable

    assert not cache.is_enable()
    assert not cache.is_enable(Command.GET)
    assert cache.is_enable(Command.SET)

    cache.disable()
    assert cache.is_disable()
    assert cache.is_disable(Command.GET)
    assert cache.is_disable(Command.SET)
    assert cache.is_full_disable

    assert not cache.is_enable()
    assert not cache.is_enable(Command.GET)
    assert not cache.is_enable(Command.SET)


async def test_disable_context_manager(cache):
    with cache.disabling(Command.INCR):
        await cache.set("test", 10)
        await cache.incr("test")
    assert await cache.get("test") == 10


async def test_disable_context_manage_get(cache):
    await cache.set("test", 10)
    with cache.disabling():
        assert await cache.get("test") is None


async def test_disable_context_manage_decor(cache):
    @cache(ttl="1m")
    async def func():
        return random()

    was = await func()
    with cache.disabling():
        assert await func() != was


async def test_init_disable(cache):
    cache.setup("mem://localhost?disable=1")
    assert cache.is_disable()


async def test_init_enable(cache):
    cache.setup("mem://localhost?enable=1")
    assert cache.is_enable()
    assert not cache.is_disable()


async def test_prefix_cache(cache):
    await cache.set("-:key", "value")

    cache.setup("://", prefix="-", disable=True)
    assert not cache.is_disable()
    assert cache.is_disable(prefix="-")

    await cache.set("key", "value")
    await cache.set("-:key", "new")

    assert await cache.get("key") == "value"
    assert await cache.get("-:key") is None

    cache.enable(prefix="-")
    assert cache.is_enable(prefix="-")

    assert await cache.get("-:key") is None  # new backend haven't this key


async def test_disable_ctz(cache):
    cache.enable()

    async def test():
        await cache.set("test", "1")
        assert await cache.get("test") == "1"
        cache.disable(Command.SET)
        await cache.set("test", "2")

    await asyncio.create_task(test())
    assert await cache.get("test") == "1"
    await cache.set("test", "3")
    assert await cache.get("test") == "3"


async def test_disable_decorators(cache: Cache, target):
    cache.disable()
    data = (i for i in range(10))
    target.is_full_disable = False

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

    @cache.bloom(capacity=10)
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

    @cache(ttl=1)
    async def func():
        return next(data)

    assert cache.is_enable()
    assert cache.is_enable(Command.SET, prefix="cache")
    assert cache.is_enable(Command.GET, prefix="cache")
    assert cache.is_enable(Command.SET, prefix="")
    assert await func() == 0
    assert await func() == 0

    cache.disable(Command.GET)

    assert not cache.is_enable(Command.GET)
    assert cache.is_enable(Command.SET)

    assert await func() == 1
    assert await func() == 2

    cache.enable(Command.GET)
    assert await func() == 2


async def test_disable_decorator_set(cache):
    data = (i for i in range(10))
    cache.disable(Command.SET)

    @cache(ttl=1)
    async def func():
        return next(data)

    assert await func() == 0
    assert await func() == 1

    cache.enable(Command.SET)
    assert await func() == 2
    assert await func() == 2


async def test_disable_and_get_enable(cache):
    data = (i for i in range(10))
    cache.enable()
    assert cache.is_enable()
    assert not cache.is_full_disable

    @cache(ttl=1)
    async def func():
        return next(data)

    assert await func() == 0
    assert await func() == 0
    cache.disable()
    assert await func() == 1
    assert await func() == 2
    cache.enable(Command.GET)
    assert await func() == 0
