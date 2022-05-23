import asyncio
from unittest.mock import Mock, PropertyMock

import pytest

from cashews.backends.memory import Memory
from cashews.disable_control import ControlMixin
from cashews.formatter import get_templates_for_func
from cashews.helpers import add_prefix, all_keys_lower
from cashews.wrapper import Cache, _create_auto_init

pytestmark = pytest.mark.asyncio


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


async def test_init_disable(cache):
    await cache.init("mem://localhost?disable=1")
    assert cache.is_disable()


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


async def test_prefix_many(cache):
    await cache.init("mem://")
    await cache.init("mem://", prefix="-")

    await cache.set("key", "value")
    await cache.set("-:key", "-value")

    assert await cache.get("key") == "value"
    assert await cache.get("-:key") == "-value"

    assert await cache.get_many("key", "-:key") == ("value", "-value")


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


async def test_disable_bloom(cache: Cache, target):
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


async def test_init(cache):
    await cache.init("mem://localhost")
    assert cache.is_enable()


async def test_auto_init(cache):
    target = Mock(wraps=Memory())
    init = False

    def set_init():
        async def _set():
            nonlocal init
            await asyncio.sleep(0.01)
            init = True

        return _set()

    type(target).is_init = PropertyMock(side_effect=lambda: init)
    target.init.side_effect = set_init
    cache._backends[""] = (target, (_create_auto_init(),))
    await asyncio.gather(cache.ping(), cache.ping(), cache.get("test"))
    target.init.assert_called_once()


async def test_all_keys_lower(cache: Cache, target):
    cache._backends[""] = cache._backends[""][0], (all_keys_lower(),)
    await cache.get(key="KEY")
    target.get.assert_called_once_with(key="key", default=None)

    await cache.set(key="KEY", value="value")
    target.set.assert_called_once_with(
        key="key",
        value="value",
        exist=None,
        expire=None,
    )
    await cache.ping()
    target.ping.assert_called_once_with(message=b"PING")


async def test_add_prefix(cache: Cache, target):
    cache._backends[""] = cache._backends[""][0], (add_prefix("prefix!"),)

    await cache.get(key="key")
    target.get.assert_called_once_with(key="prefix!key", default=None)

    await cache.set(key="key", value="value")
    target.set.assert_called_once_with(
        key="prefix!key",
        value="value",
        exist=None,
        expire=None,
    )
    await cache.ping()
    target.ping.assert_called_once_with(message=b"PING")


async def test_add_prefix_get_many(cache: Cache, target):
    cache._backends[""] = cache._backends[""][0], (add_prefix("prefix!"),)
    await cache.get_many("key")
    target.get_many.assert_called_once_with("prefix!key")


async def test_add_prefix_delete_match(cache: Cache, target):
    cache._backends[""] = cache._backends[""][0], (add_prefix("prefix!"),)
    await cache.delete_match("key")
    target.delete_match.assert_called_once_with(pattern="prefix!key")


async def test_smoke_cmds(cache: Cache, target):
    await cache.set(key="key", value={"any": True}, expire=60, exist=None)
    target.set.assert_called_once_with(key="key", value={"any": True}, expire=60, exist=None)

    await cache.get("key")  # -> Any
    target.get.assert_called_once_with(key="key", default=None)

    await cache.get_many("key1", "key2")
    target.get_many.assert_called_once_with("key1", "key2", default=None)

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

    await cache.get_bits("key", 1, 2, 3, size=2)
    target.get_bits.assert_called_once_with("key", 1, 2, 3, size=2)

    await cache.incr_bits("key", 1, 2, 3, size=2)
    target.incr_bits.assert_called_once_with("key", 1, 2, 3, size=2, by=1)

    [key async for key in cache.keys_match("key:*")]
    target.keys_match.assert_called_once_with("key:*")

    [key_value async for key_value in cache.get_match("key:*")]
    target.get_match.assert_called_once_with("key:*", batch_size=100, default=None)


async def test_disable_cache_on_fail_return(cache: Cache):
    assert await cache.get("key") is None

    @cache(ttl=1, key="key", upper=True)
    @cache.failover(ttl=1, key="fail", prefix="")
    async def func(fail=False):
        if fail:
            raise Exception()
        return "1"

    await cache.set("fail", "test_fail")

    assert await func(fail=True) == "test_fail"  # return from fail cache but simple cache should be skipped
    assert await cache.get("key") is None

    await cache.delete("fail")
    await cache.set("key", "val")
    assert await func() == "val"


async def test_disable_cache_on_fail_return_2(cache: Cache):
    assert await cache.get("key") is None

    @cache.failover(ttl=1, key="fail", prefix="")
    @cache(ttl=1, key="key")
    async def func(fail=False):
        if fail:
            raise Exception()
        return "1"

    await cache.set("fail", "test_fail")

    assert await func(fail=True) == "test_fail"  # return from fail cache but simple cache should be skipped
    assert await cache.get("key") is None

    await cache.delete("fail")
    assert await cache.get("fail") is None
    await cache.set("key", "val")
    assert await func() == "val"
    assert await cache.get("fail") is None


async def test_multilayer_cache(cache: Cache):
    # If results from key2, key1 must not be set

    @cache(ttl=1, key="key1", upper=True)
    @cache(ttl=1, key="key2")
    async def func():
        return 1

    await cache.set("key2", "test2")

    assert await func() == "test2"
    assert await cache.get("key1") is None

    await cache.set("key1", "test1")
    assert await func() == "test1"
    assert await cache.get("key2") == "test2"


async def test_cache_decor_register(cache: Cache):
    @cache(ttl=1, key="key:{val}", prefix="test")
    async def my_func(val=1):
        return val

    assert next(get_templates_for_func(my_func)) == "test:key:{val}"


async def test_cache_lock():
    m = Mock()
    cache = Cache()
    cache.setup("mem://")

    @cache(ttl=5, lock=True)
    async def my_func(val=1):
        await asyncio.sleep(0)  # for task switching
        m(val)
        return val

    await asyncio.gather(my_func(), my_func(), my_func())

    m.assert_called_once_with(1)
