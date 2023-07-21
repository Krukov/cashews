import asyncio
from decimal import Decimal

import pytest

from cashews import Cache
from cashews.backends.interface import NOT_EXIST, UNLIMITED
from cashews.backends.memory import Memory

pytestmark = pytest.mark.asyncio
VALUE = Decimal("100.2")


async def test_set_get(cache: Cache):
    await cache.set("key", VALUE)
    assert await cache.get("key") == VALUE


async def test_set_get_bytes(cache: Cache):
    await cache.set("key", b"10")
    assert await cache.get("key") == b"10"


async def test_incr_get(cache: Cache):
    await cache.incr("key")
    assert await cache.get("key") == 1


async def test_incr_value_get(cache: Cache):
    await cache.incr("key")
    await cache.incr("key", 2)
    assert await cache.get("key") == 3


async def test_incr_expire(cache: Cache):
    await cache.incr("key", expire=10)
    assert await cache.get_expire("key") == 10


async def test_set_get_many(cache: Cache):
    await cache.set("key", VALUE)
    assert await cache.get_many("key", "no_exists") == (VALUE, None)


async def test_diff_types_get_many(cache: Cache):
    await cache.incr("key")
    await cache.incr_bits("key2", 0)
    assert await cache.get_many("key", "key2") == (1, None)


async def test_set_exist(cache: Cache):
    assert await cache.set("key", "value")
    assert await cache.set("key", VALUE, exist=True)
    assert await cache.get("key") == VALUE

    assert not await cache.set("key2", "value", exist=True)
    assert await cache.get("key2") is None

    assert await cache.set("key2", "value", exist=False)
    assert await cache.get("key2") == "value"

    assert not await cache.set("key2", "value2", exist=False)
    assert await cache.get("key2") == "value"


async def test_set_many(cache: Cache):
    await cache.set_many({"key1": VALUE, "key2": "value2"}, expire=1)
    assert await cache.get("key1", VALUE)
    assert await cache.get("key2", "value2")


async def test_get_no_value(cache: Cache):
    assert await cache.get("key2") is None
    assert await cache.get("key2", default=VALUE) is VALUE


async def test_incr(cache: Cache):
    assert await cache.incr("incr") == 1
    assert await cache.incr("incr") == 2
    assert await cache.incr("incr", 5) == 7
    assert await cache.get("incr") == 7


async def test_incr_set(cache: Cache):
    await cache.set("incr", "test")
    with pytest.raises(Exception):
        assert await cache.incr("incr") == 1
    assert await cache.get("incr") == "test"


async def test_ping(cache: Cache):
    assert await cache.ping() == b"PONG"


async def test_exists(cache: Cache):
    assert not await cache.exists("not")
    await cache.set("yes", VALUE)
    assert await cache.exists("yes")


async def test_expire(cache: Cache):
    await cache.set("key", VALUE, expire=0.01)
    assert await cache.get("key") == VALUE
    await asyncio.sleep(0.01)
    assert await cache.get("key") is None


async def test_get_set_expire(cache: Cache):
    assert await cache.get_expire("key") == NOT_EXIST
    await cache.set("key", VALUE)
    assert await cache.get("key") == VALUE
    assert await cache.get_expire("key") == UNLIMITED
    await cache.expire("key", 1)
    assert await cache.get_expire("key") == 1


async def test_delete_many(cache: Cache):
    await cache.set("key1", VALUE)
    await cache.set("key2", VALUE)
    await cache.set("key3", VALUE)

    await cache.delete_many("key1", "key2", "key4")

    assert await cache.get("key1") is None
    assert await cache.get("key2") is None
    assert await cache.get("key3") == VALUE


async def test_delete_match(cache: Cache):
    await cache.set("pref:test:test", VALUE)
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


async def test_scan(cache: Cache):
    await cache.set("pref:test:test", VALUE)
    await cache.set("pref:value:test", b"value2")
    await cache.set("pref:-:test", b"-")
    await cache.set("pref:*:test", b"*")

    await cache.set("ppref:test:test", b"value3")
    await cache.set("pref:test:tests", b"value3")

    keys = [key async for key in cache.scan("pref:*:test")]

    assert len(keys) == 4
    assert set(keys) == {"pref:test:test", "pref:value:test", "pref:-:test", "pref:*:test"}


async def test_get_match(cache: Cache):
    await cache.set("pref:test:test", VALUE)
    await cache.set("pref:value:test", b"value2")
    await cache.set("pref:-:test", b"-")
    await cache.set("pref:*:test", b"*")

    await cache.set("ppref:test:test", b"value3")
    await cache.set("pref:test:tests", b"value3")

    match = [(key, value) async for key, value in cache.get_match("pref:*:test")]

    assert len(match) == 4
    assert dict(match) == {
        "pref:test:test": VALUE,
        "pref:value:test": b"value2",
        "pref:-:test": b"-",
        "pref:*:test": b"*",
    }
    match = [(key, value) async for key, value in cache.get_match("not_exists:*")]
    assert len(match) == 0


async def test_set_lock_unlock(cache: Cache):
    await cache.set_lock("lock", "lock", 10)
    assert await cache.is_locked("lock")
    await cache.unlock("lock", "lock")
    assert not await cache.is_locked("lock")


async def test_lock(cache: Cache):
    async with cache.lock("lock", 10):
        assert await cache.is_locked("lock")

    assert not await cache.is_locked("lock")


async def test_diff_types_get_match(cache: Cache):
    await cache.incr("key")
    await cache.incr_bits("key2", 0)
    match = [(key, value) async for key, value in cache.get_match("*")]
    assert len(match) == 1, match
    assert dict(match) == {"key": 1}


async def test_get_size(cache: Cache):
    await cache.set("test", b"1")
    assert isinstance(await cache.get_size("test"), int)


async def test_get_keys_count(cache: Cache):
    await cache.set("test", b"1")
    assert await cache.get_keys_count() == 1


async def test_get_bits(cache: Cache):
    assert await cache.get_bits("test", 0, 2, 10, 50000, size=1) == (0, 0, 0, 0)
    assert await cache.get_bits("test", 0, 1, 3, size=15) == (
        0,
        0,
        0,
    )


async def test_incr_bits(cache: Cache):
    await cache.incr_bits("test", 0, 1, 4)
    assert await cache.get_bits("test", 0, 1, 2, 3, 4) == (1, 1, 0, 0, 1)


async def test_bits_size(cache: Cache):
    await cache.incr_bits("test", 0, 1, 4, size=5, by=3)
    assert await cache.get_bits("test", 0, 1, 2, 3, 4, size=5) == (3, 3, 0, 0, 3)


async def test_slice_incr(cache: Cache):
    assert await cache.slice_incr("test", 0, 5, maxvalue=10) == 1
    assert await cache.slice_incr("test", 1, 6, maxvalue=10) == 2
    assert await cache.slice_incr("test", 2, 7, maxvalue=10) == 3
    assert await cache.slice_incr("test", 3, 8, maxvalue=10) == 4
    assert await cache.slice_incr("test", 5, 10, maxvalue=4) == 4
    assert await cache.slice_incr("test", 9, 11, maxvalue=10) == 1
    assert await cache.slice_incr("test", 15, 20, maxvalue=10) == 1
    assert await cache.slice_incr("test", 9, 11, 10) == 1


async def test_lru(backend_factory):
    cache = backend_factory(Memory, size=10)
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

    await cache.close()


async def test_lru2(backend_factory):
    cache = backend_factory(Memory, size=10)
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

    await cache.close()
