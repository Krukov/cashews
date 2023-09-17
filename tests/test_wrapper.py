import asyncio
from unittest.mock import Mock, PropertyMock

import pytest

from cashews.backends.memory import Memory
from cashews.cache_condition import NOT_NONE
from cashews.exceptions import NotConfiguredError
from cashews.formatter import get_templates_for_func
from cashews.wrapper import Cache
from cashews.wrapper.auto_init import create_auto_init

pytestmark = pytest.mark.asyncio


async def test_prefix_many(cache):
    await cache.init("mem://")
    await cache.init("mem://", prefix="-")

    await cache.set("key", "value")
    await cache.set("-:key", "-value")

    assert await cache.get("key") == "value"
    assert await cache.get("-:key") == "-value"

    assert await cache.get_many("key", "-:key") == ("value", "-value")


async def test_init():
    cache = Cache()
    assert cache.is_init
    cache.setup("mem://localhost", prefix="test")
    assert not cache.is_init

    await cache.init("mem://localhost", prefix="new")
    assert cache.is_init

    cache.setup("mem://localhost")
    assert not cache.is_init


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
    cache._backends[""] = (target, (create_auto_init(),))
    await asyncio.gather(cache.ping(), cache.ping(), cache.get("test"))
    target.init.assert_called_once()


async def test_smoke_cmds(cache: Cache, target: Mock):
    await cache.set(key="key", value={"any": True}, expire=60, exist=None)
    target.set.assert_called_once_with(
        key="key",
        value={"any": True},
        expire=60,
        exist=None,
    )

    await cache.set_raw(key="key2", value="value", expire=60)
    target.set_raw.assert_called_once_with(key="key2", value="value", expire=60)

    await cache.get("key")  # -> Any
    target.get.assert_called_once_with(key="key", default=None)

    await cache.get_raw("key")  # -> Any
    target.get_raw.assert_called_once_with(key="key")

    await cache.set_many({"key1": "value1", "key2": "value2"}, expire=60)
    target.set_many.assert_called_once_with(pairs={"key1": "value1", "key2": "value2"}, expire=60)

    await cache.get_many("key1", "key2")
    target.get_many.assert_called_once_with("key1", "key2", default=None)

    await cache.incr("key_incr")  # -> int
    target.incr.assert_called_once_with(key="key_incr", value=1, expire=None)

    await cache.delete("key")
    target.delete.assert_called_once_with(key="key")

    await cache.delete_match("key*")
    target.delete_match.assert_called_once_with(pattern="key*")

    await cache.delete_many("key", "key2")
    target.delete_many.assert_called_once_with("key", "key2")

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

    await cache.get_bits("bits_key", 1, 2, 3, size=2)
    target.get_bits.assert_called_once_with("bits_key", 1, 2, 3, size=2)

    await cache.incr_bits("bits_key", 1, 2, 3, size=2)
    target.incr_bits.assert_called_once_with("bits_key", 1, 2, 3, size=2, by=1)

    await cache.set("key", "value")
    assert [key async for key in cache.scan("key*")] == ["key"]
    target.scan.assert_called_once_with("key*", batch_size=100)

    assert [key_value async for key_value in cache.get_match("key*")] == [("key", "value")]
    target.get_match.assert_called_once_with("key*", batch_size=100)

    await cache.get_size("key")
    target.get_size.assert_called_once_with(key="key")

    await cache.slice_incr("key_slice", 0, 10, maxvalue=10)
    target.slice_incr.assert_called_once_with(key="key_slice", start=0, end=10, maxvalue=10, expire=None)

    await cache.set_pop("key_set", count=10)
    target.set_pop.assert_called_once_with(key="key_set", count=10)

    await cache.set_add("key_set", "val1", "val2")
    target.set_add.assert_called_once_with("key_set", "val1", "val2", expire=None)

    await cache.close()


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

    @cache(ttl=1, key="key1", upper=True, lock=True)
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


async def test_cache_lock(cache: Cache):
    m = Mock()

    @cache(ttl=3, lock=True)
    async def my_func(val=1):
        await asyncio.sleep(0)  # for task switching
        m(val)
        return val

    await asyncio.gather(my_func(), my_func(), my_func())

    m.assert_called_once_with(1)


_cache = Cache()
_cache.setup("mem://")


@pytest.mark.parametrize("decorator", (_cache, _cache.early, _cache.soft))
async def test_time_condition(decorator):
    m = Mock()

    @decorator(key="test", ttl=10, time_condition=0.1, protected=False)
    async def my_func(sleep=0.01):
        await asyncio.sleep(sleep)
        m()
        return sleep

    await my_func()
    await asyncio.gather(my_func(), my_func(), my_func())
    assert m.call_count == 4
    m.reset_mock()

    await asyncio.gather(my_func(0.15), my_func(), my_func(0.1))
    assert m.call_count == 3
    await my_func(0.15)
    await my_func()
    assert m.call_count == 3

    m.reset_mock()
    await _cache.clear()
    await my_func()
    assert m.call_count == 1


async def test_cache_simple_not_none(cache: Cache):
    mock = Mock()

    @cache(ttl=0.1, key="key", condition=NOT_NONE)
    async def func():
        mock()
        return None

    assert await func() is None
    assert mock.call_count == 1

    assert await func() is None
    assert mock.call_count == 2


async def test_no_setup():
    cache = Cache()

    with pytest.raises(NotConfiguredError):
        await cache.get("test")


async def test_no_setup_decor():
    cache = Cache()

    @cache(ttl=0.1, key="key")
    async def func():
        return None

    with pytest.raises(NotConfiguredError):
        await func()
