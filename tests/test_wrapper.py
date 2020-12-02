import asyncio
import random
from unittest.mock import Mock

import pytest
from cashews.backends.memory import Memory
from cashews.disable_control import ControlMixin
from cashews.helpers import add_prefix
from cashews.wrapper import Cache, _auto_init


@pytest.fixture(name="target")
def _target():
    class New(ControlMixin, Memory):
        pass

    return Mock(wraps=New())


@pytest.fixture(name="cache")
def __cache(target):
    _cache = Cache()
    _cache._add_backend(Memory)
    _cache._backends[""] = (target, _cache._backends[""][1])
    return _cache


@pytest.mark.asyncio
async def test_init_disable(cache):
    await cache.init("mem://localhost?disable=1")
    assert cache.is_disable()


@pytest.mark.asyncio
async def test_prefix(cache):
    await cache.init("mem://localhost")
    await cache.init("://", prefix="-")
    assert not cache.is_disable()
    assert cache.is_disable(prefix="-")

    await cache.set("key", "value")
    await cache.set("-:key", "-value")

    assert await cache.get("key") == "value"
    assert await cache.get("-:key") == None
    assert await cache.get("-:key", default="def") == "def"


@pytest.mark.asyncio
async def test_disable_cmd(cache):
    await cache.init("mem://localhost")
    cache.disable("incr")
    await cache.set("test", 10)
    await cache.incr("test")
    assert await cache.get("test") == 10

    cache.enable("incr")
    await cache.incr("test")
    assert await cache.get("test") == 11


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_disable_decorators(cache: Cache, target):
    cache.disable()
    data = (i for i in range(10))

    @cache(ttl=1)
    @cache.fail(ttl=1)
    @cache.hit(ttl=1, cache_hits=1)
    @cache.perf(ttl=1)
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_init(cache):
    await cache.init("mem://localhost")
    assert cache.is_enable()


@pytest.mark.asyncio
async def test_auto_init(cache):
    target = Memory()
    cache._backends[""] = target, (_auto_init,)
    assert not target.is_init
    assert b"PONG" == await cache.ping()
    assert target.is_init


@pytest.mark.asyncio
async def test_add_prefix(cache: Cache, target):
    cache._backends[""] = cache._backends[""][0], (add_prefix("prefix!"),)

    await cache.get(key="key")
    target.get.assert_called_once_with(key="prefix!key", default=None)

    await cache.set(key="key", value="value")
    target.set.assert_called_once_with(
        key="prefix!key", value="value", exist=None, expire=None,
    )
    await cache.ping()
    target.ping.assert_called_once_with(message=None)


@pytest.mark.asyncio
async def test_add_prefix_get_many(cache: Cache, target):
    cache._backends[""] = cache._backends[""][0], (add_prefix("prefix!"),)
    await cache.get_many("key")
    target.get_many.assert_called_once_with("prefix!key")


@pytest.mark.asyncio
async def test_add_prefix_delete_match(cache: Cache, target):
    cache._backends[""] = cache._backends[""][0], (add_prefix("prefix!"),)
    await cache.delete_match("key")
    target.delete_match.assert_called_once_with(pattern="prefix!key")


@pytest.mark.asyncio
async def test_smoke_cmds(cache: Cache, target):
    await cache.set(key="key", value={"any": True}, expire=60, exist=None)
    target.set.assert_called_once_with(key="key", value={"any": True}, expire=60, exist=None)

    await cache.get("key")  # -> Any
    target.get.assert_called_once_with(key="key", default=None)

    await cache.get_many("key1", "key2")
    target.get_many.assert_called_once_with("key1", "key2")

    await cache.incr("key_incr")  # -> int
    target.incr.assert_called_once_with(key="key_incr")

    await cache.delete("key")
    target.delete.assert_called_once_with(key="key")

    await cache.expire(key="key", timeout=10)
    target.expire.assert_called_once_with(key="key", timeout=10)

    await cache.get_expire(key="key")  # -> int seconds to expire
    target.get_expire.assert_called_once()

    await cache.ping(message=b"test")  # -> bytes
    target.ping.assert_called_once_with(message=b"test")

    await cache.clear()
    target.clear.assert_called_once_with()

    await cache.is_locked("key", wait=60)  # -> bool
    target.is_locked.assert_called_once_with(key="key", wait=60, step=0.1)

    await cache.set_lock("key", "value", expire=60)  # -> bool
    target.set_lock.assert_called_once_with(key="key", value="value", expire=60)

    await cache.unlock("key", "value")  # -> bool
    target.unlock.assert_called_once_with(key="key", value="value")

    await cache.exists("key")
    target.exists.assert_called_once_with(key="key")


@pytest.mark.asyncio
async def test_disable_cache_on_fail_return(cache: Cache):
    @cache(ttl=0.05, key="cache")
    @cache.failover(ttl=1, key="fail")
    async def func(fail=False):
        if fail:
            raise Exception()
        return random.randint(0, 100)

    first = await func()  # cache by fail and siple cache
    await asyncio.sleep(0.1)  # expire simple cache
    assert await func(fail=True) == first  # return from fail cache but simple cache should be skipped
    assert await func() != first
