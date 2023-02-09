import asyncio

import pytest

from cashews.backends.interface import NOT_EXIST, UNLIMITED, Backend
from cashews.backends.memory import Memory

pytestmark = pytest.mark.asyncio


async def test_set_get(cache):
    await cache.set("key", b"value")
    assert await cache.get("key") == b"value"


async def test_set_get_many(cache):
    await cache.set("key", b"value")
    assert await cache.get_many("key", "no_exists") == (b"value", None)


async def test_set_exist(cache):
    assert await cache.set("key", "value")
    assert await cache.set("key", "value2", exist=True)
    assert await cache.get("key") == "value2"

    assert not await cache.set("key2", "value", exist=True)
    assert await cache.get("key2") is None

    assert await cache.set("key2", "value", exist=False)
    assert await cache.get("key2") == "value"

    assert not await cache.set("key2", "value2", exist=False)
    assert await cache.get("key2") == "value"


async def test_set_many(cache):
    await cache.set_many({"key1": "value1", "key2": "value2"}, expire=1)
    assert await cache.get("key1", "value1")
    assert await cache.get("key2", "value2")


async def test_get_no_value(cache):
    assert await cache.get("key2") is None


async def test_incr(cache):
    assert await cache.incr("incr") == 1
    assert await cache.incr("incr") == 2
    assert await cache.get("incr") == 2


async def test_incr_set(cache):
    await cache.set("incr", "test")
    with pytest.raises(Exception):
        assert await cache.incr("incr") == 1
    assert await cache.get("incr") == "test"


async def test_ping(cache):
    assert await cache.ping() == b"PONG"


async def test_exists(cache):
    assert not await cache.exists("not")
    await cache.set("yes", "1")
    assert await cache.exists("yes")


async def test_expire(cache):
    await cache.set("key", b"value", expire=0.01)
    assert await cache.get("key") == b"value"
    await asyncio.sleep(0.1)
    assert await cache.get("key") is None


async def test_get_set_expire(cache):
    assert await cache.get_expire("key") == NOT_EXIST
    await cache.set("key", b"value")
    assert await cache.get("key") == b"value"
    assert await cache.get_expire("key") == UNLIMITED
    await cache.expire("key", 1)
    assert await cache.get_expire("key") == 1


async def test_delete_many(cache: Backend):
    await cache.set("key1", b"value")
    await cache.set("key2", b"value")
    await cache.set("key3", b"value")

    await cache.delete_many("key1", "key2", "key4")

    assert await cache.get("key1") is None
    assert await cache.get("key2") is None
    assert await cache.get("key3") == b"value"


async def test_delete_match(cache: Backend):
    await cache.set("pref:test:test", b"value")
    await cache.set("pref:value:test", b"value2")
    await cache.set("pref:-:test", b"-")
    await cache.set("pref:*:test", b"*")

    await cache.set("ppref:test:test", b"value3")
    await cache.set("pref:test:tests", b"value3")

    await cache.delete_match("pref:*:test")

    assert await cache.get("pref:test:test") is None
    assert await cache.get("pref:value:test") is None
    assert await cache.get("pref:-:test") is None
    assert await cache.get("pref:*:test") is None

    assert await cache.get("ppref:test:test") is not None
    assert await cache.get("pref:test:tests") is not None


async def test_scan(cache: Backend):
    await cache.set("pref:test:test", b"value")
    await cache.set("pref:value:test", b"value2")
    await cache.set("pref:-:test", b"-")
    await cache.set("pref:*:test", b"*")

    await cache.set("ppref:test:test", b"value3")
    await cache.set("pref:test:tests", b"value3")

    keys = [key async for key in cache.scan("pref:*:test")]

    assert len(keys) == 4
    assert set(keys) == {"pref:test:test", "pref:value:test", "pref:-:test", "pref:*:test"}


async def test_get_match(cache: Backend):
    await cache.set("pref:test:test", b"value")
    await cache.set("pref:value:test", b"value2")
    await cache.set("pref:-:test", b"-")
    await cache.set("pref:*:test", b"*")

    await cache.set("ppref:test:test", b"value3")
    await cache.set("pref:test:tests", b"value3")

    match = [(key, value) async for key, value in cache.get_match("pref:*:test")]

    assert len(match) == 4
    assert dict(match) == {
        "pref:test:test": b"value",
        "pref:value:test": b"value2",
        "pref:-:test": b"-",
        "pref:*:test": b"*",
    }
    match = [(key, value) async for key, value in cache.get_match("not_exists:*")]
    assert len(match) == 0


async def test_get_size(cache: Backend):
    await cache.set("test", b"1")
    assert isinstance(await cache.get_size("test"), int)


async def test_get_bits(cache: Backend):
    assert await cache.get_bits("test", 0, 2, 10, 50000, size=1) == (0, 0, 0, 0)
    assert await cache.get_bits("test", 0, 1, 3, size=15) == (
        0,
        0,
        0,
    )


async def test_incr_bits(cache: Backend):
    await cache.incr_bits("test", 0, 1, 4)
    assert await cache.get_bits("test", 0, 1, 2, 3, 4) == (1, 1, 0, 0, 1)


async def test_bits_size(cache: Backend):
    await cache.incr_bits("test", 0, 1, 4, size=5, by=3)
    assert await cache.get_bits("test", 0, 1, 2, 3, 4, size=5) == (3, 3, 0, 0, 3)


async def test_slice_incr(cache: Backend):
    assert await cache.slice_incr("test", 0, 5, maxvalue=10) == 1
    assert await cache.slice_incr("test", 1, 6, maxvalue=10) == 2
    assert await cache.slice_incr("test", 2, 7, maxvalue=10) == 3
    assert await cache.slice_incr("test", 3, 8, maxvalue=10) == 4
    assert await cache.slice_incr("test", 5, 10, maxvalue=4) == 4
    assert await cache.slice_incr("test", 9, 11, maxvalue=10) == 1
    assert await cache.slice_incr("test", 15, 20, maxvalue=10) == 1
    assert await cache.slice_incr("test", 9, 11, 10) == 1


async def test_lru(backend_factory):
    cache = await backend_factory(Memory, size=10)
    # fill cache
    for i in range(10):
        await cache.set(f"key:{i}", i)

    # use only 5 first
    for i in range(5):
        await cache.get(f"key:{i}")

    # add 5 more keys
    for i in range(5):
        await cache.set(f"key:{i}:new", i)

    assert len(cache.store) == 10

    for i in range(5):
        assert await cache.get(f"key:{i}") == i

    for i in range(6, 10):
        assert await cache.get(f"key:{i}") is None


async def test_lru2(backend_factory):
    cache = await backend_factory(Memory, size=10)
    # fill cache
    for i in range(10):
        await cache.set(f"key:{i}", i)

    # use only 5 last
    for i in range(6, 10):
        await cache.get(f"key:{i}")

    # add 5 more keys
    for i in range(5):
        await cache.set(f"key:{i}:new", i)

    assert len(cache.store) == 10

    for i in range(5):
        assert await cache.get(f"key:{i}") is None

    for i in range(6, 10):
        assert await cache.get(f"key:{i}") == i
