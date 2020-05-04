import asyncio
from unittest.mock import Mock, patch

import pytest
from cashews.backends.interface import Backend
from cashews.backends.memory import Memory
from cashews.helpers import add_prefix
from cashews.wrapper import Cache


@pytest.fixture(name="target")
def _target():
    return Mock(wraps=Memory())


@pytest.fixture(name="cache")
def __cache(target):
    _cache = Cache()
    _cache._target = target
    return _cache


def test_setup(cache):
    backend = Mock()
    cache._target = None
    with patch("cashews.wrapper.Memory", backend):
        cache.setup("mem://localhost?test=1")
    backend.assert_called_with(test=1)


@pytest.mark.asyncio
async def test_init_disable(cache):
    backend = Mock(wraps=Backend())
    with patch("cashews.wrapper.Memory", Mock(return_value=backend)):
        await cache.init("mem://localhost?disable=1")
    backend.init.assert_not_called()
    assert cache.is_disable()


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
    cache.enable("cmds")

    async def test():
        await cache.set("test", "1")
        assert await cache.get("test") == "1"
        cache.disable("cmds")
        await cache.set("test", "2")

    await asyncio.create_task(test())
    assert await cache.get("test") == "1"
    await cache.set("test", "3")
    assert await cache.get("test") == "3"


@pytest.mark.asyncio
async def test_disable_decorators(cache: Cache, target):
    cache.disable("decorators")
    data = (i for i in range(10))

    @cache(ttl=1)
    @cache.fail(1)
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

    cache.enable("decorators")
    assert await func() == 2
    target.get.assert_called()
    target.set.assert_called()
    assert await func() == 2


@pytest.mark.asyncio
async def test_init(cache):
    backend = Mock(wraps=Backend())
    with patch("cashews.wrapper.Memory", Mock(return_value=backend)):
        await cache.init("mem://localhost")
    backend.init.assert_called_once()
    assert cache.is_enable()


@pytest.mark.asyncio
async def test_auto_init(cache, target):
    await cache.ping()
    target.init.assert_called_once()


@pytest.mark.asyncio
async def test_add_prefix(cache: Cache, target):
    cache.middlewares = (add_prefix("prefix!"),)

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
    cache.middlewares = (add_prefix("prefix!"),)
    await cache.get_many("key")
    target.get_many.assert_called_once_with("prefix!key")


@pytest.mark.asyncio
async def test_add_prefix_delete_match(cache: Cache, target):
    cache.middlewares = (add_prefix("prefix!"),)
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
