import asyncio
import random

import pytest

from cashews import LockedError
from cashews.backends.interface import NOT_EXIST, UNLIMITED
from cashews.wrapper import Cache
from cashews.wrapper.transaction import TransactionMode

pytestmark = pytest.mark.asyncio


@pytest.fixture(
    name="tx_mode",
    params=[
        TransactionMode.FAST,
        TransactionMode.LOCKED,
        TransactionMode.SERIALIZABLE,
    ],
)
def _tx_mode(request):
    return request.param


async def test_transaction_set(cache: Cache, tx_mode):
    await cache.set("key1", "value1", expire=1)
    await cache.set("key2", "value2", expire=2)

    async with cache.transaction(tx_mode):
        await cache.set("key1", "value2", expire=3)
        await cache.set("key", "value")

        assert await cache.get("key") == "value"
        assert await cache.get("key1") == "value2"
        assert await cache.get("key2") == "value2"

    assert await cache.get("key") == "value"
    assert await cache.get("key1") == "value2"
    assert await cache.get("key2") == "value2"


async def test_transaction_set_rollback(cache: Cache, tx_mode):
    await cache.set("key1", "value1", expire=1)
    await cache.set("key2", "value2", expire=2)

    async with cache.transaction(tx_mode) as tx:
        await cache.set("key1", "value2", expire=3)
        await cache.set("key", "value")

        assert await cache.get("key") == "value"
        assert await cache.get("key1") == "value2"
        assert await cache.get("key2") == "value2"
        await tx.rollback()

        assert await cache.get("key") is None
        assert await cache.get("key1") == "value1"
        assert await cache.get("key2") == "value2"

    assert await cache.get("key") is None
    assert await cache.get("key1") == "value1"
    assert await cache.get("key2") == "value2"


async def test_transaction_set_exception(cache: Cache, tx_mode):
    await cache.set("key1", "value1", expire=1)
    await cache.set("key2", "value2", expire=2)

    with pytest.raises(ValueError):
        async with cache.transaction(tx_mode):
            await cache.set("key1", "value2", expire=3)
            await cache.set("key", "value")

            assert await cache.get("key") == "value"
            assert await cache.get("key1") == "value2"
            assert await cache.get("key2") == "value2"
            raise ValueError("rollback")

    assert await cache.get("key") is None
    assert await cache.get("key1") == "value1"
    assert await cache.get("key2") == "value2"


async def test_transaction_set_delete_get_many(cache: Cache, tx_mode):
    await cache.set("key1", "value", expire=1)
    await cache.set("key2", "value2", expire=2)

    async with cache.transaction(tx_mode):
        await cache.delete("key1")
        assert await cache.get("key1") is None

        await cache.set_many({"key1": "value1", "key3": "value3"}, expire=3)
        await cache.delete("key")
        await cache.delete_many("key2", "key3")

        assert await cache.get("key") is None
        assert await cache.get("key1") == "value1"
        assert await cache.get("key2") is None
        assert await cache.get("key3") is None

        await cache.set("key2", "value2")
        assert await cache.get_many("key", "key1", "key2") == (None, "value1", "value2")

    assert await cache.get("key") is None
    assert await cache.get("key1") == "value1"
    assert await cache.get("key2") == "value2"
    assert await cache.get("key3") is None
    assert await cache.get_many("key", "key1", "key2") == (None, "value1", "value2")


async def test_transaction_set_delete_match_get_many_match(cache: Cache, tx_mode):
    await cache.set("kek", "value")
    await cache.set("key1", "value", expire=1)
    await cache.set("key2", "value2", expire=2)

    async with cache.transaction(tx_mode):
        await cache.set("key1", "value1", expire=3)
        await cache.set("key3", "value3")
        await cache.delete_match("key*")

        assert await cache.get("key1") is None
        assert await cache.get("key2") is None
        assert await cache.get("key3") is None

        await cache.set("key2", "value2")
        assert await cache.get_many("kek", "key1", "key2") == ("value", None, "value2")
        match = sorted([(key, value) async for key, value in cache.get_match("k*")])
        assert len(match) == 2
        assert match[0] == ("kek", "value")
        assert match[1] == ("key2", "value2")

    assert await cache.get("key1") is None
    assert await cache.get("key2") == "value2"
    assert await cache.get("key3") is None

    assert await cache.get_many("kek", "key1", "key2") == ("value", None, "value2")


async def test_transaction_incr(cache: Cache, tx_mode):
    await cache.incr("key1")
    await cache.incr("key3")
    await cache.incr("key4")

    async with cache.transaction(tx_mode):
        assert await cache.incr("key1") == 2
        assert await cache.incr("key1") == 3

        assert await cache.incr("key2") == 1

        assert await cache.delete("key3")
        assert await cache.incr("key3") == 1

        assert await cache.incr("key4") == 2
        assert await cache.delete("key4")

    assert await cache.incr("key1") == 4
    assert await cache.incr("key2") == 2
    assert await cache.incr("key3") == 2
    assert await cache.incr("key4") == 1


async def test_transaction_expire(cache: Cache, tx_mode):
    await cache.set("key1", "value1")
    await cache.set("key3", "value3")
    await cache.set("key4", "value4")

    async with cache.transaction(tx_mode):
        await cache.delete("key3")
        await cache.set("key2", "value2", expire=100)
        await cache.expire("key1", 11)
        await cache.expire("key2", 12)
        await cache.expire("key3", 13)

        assert await cache.get_expire("key1") >= 10
        assert await cache.get_expire("key2") >= 11
        assert await cache.get_expire("key3") < 0
        assert await cache.get_expire("key4") == UNLIMITED
        assert await cache.get_expire("key5") == NOT_EXIST

    assert await cache.get_expire("key1") >= 10
    assert await cache.get_expire("key2") >= 11
    assert await cache.get_expire("key3") < 0


async def test_transaction_exist(cache: Cache, tx_mode):
    await cache.set("key1", "value1")
    await cache.set("key3", "value3")

    async with cache.transaction(tx_mode):
        await cache.delete("key3")
        await cache.set("key2", "value2")

        assert not await cache.exists("key3")
        assert await cache.exists("key1")
        assert await cache.exists("key2")

    assert await cache.exists("key1")
    assert await cache.exists("key2")
    assert not await cache.exists("key3")


async def test_transaction_scan(cache: Cache, tx_mode):
    await cache.set("key1", "value1")
    await cache.set("key3", "value3")

    async with cache.transaction(tx_mode):
        await cache.delete("key3")
        await cache.set("key2", "value2")

        assert sorted([key async for key in cache.scan("k*")]) == ["key1", "key2"]

    assert sorted([key async for key in cache.scan("k*")]) == ["key1", "key2"]


async def test_isolation(cache: Cache, tx_mode):
    await cache.set("key1", "value1")
    await cache.set("key2", "value2")

    @cache.transaction(mode=tx_mode, timeout=1)
    async def do(value: str):
        assert await cache.get("key2") == "value2"  # no locks
        await cache.incr("incr")
        await cache.set("key1", "value")
        if value == "v4":
            raise Exception("rollback")
        await cache.set("key2", value)
        await asyncio.sleep(random.randint(1, 5) / 100)  # like race condition
        await cache.incr("incr")
        await cache.set("key3", value, exist=True)
        await cache.set("key1", value)

    await asyncio.gather(
        do("v1"),
        do("v2"),
        do("v3"),
        do("v4"),
        return_exceptions=True,
    )

    if tx_mode == TransactionMode.FAST:
        assert await cache.get("incr") == 2
    else:
        assert await cache.get("incr") == 6

    first_value = None
    async for key, value in cache.get_match("k*"):
        if first_value is None:
            first_value = value
        else:
            assert value == first_value


async def test_inner_transaction_commit_outer_rollback(cache: Cache, tx_mode):
    await cache.set("key1", "value1")
    await cache.set("key2", "value2")

    async with cache.transaction(tx_mode) as tx:
        await cache.delete("key2")
        await cache.set("key1", "value2")
        async with cache.transaction(tx_mode):
            await cache.set("key1", "value3")
        await tx.rollback()

    assert await cache.get("key1") == "value1"
    assert await cache.get("key2") == "value2"


async def test_gather(cache: Cache, tx_mode):
    await cache.set("key1", "value1")
    await cache.set("key2", "value1")

    async with cache.transaction(tx_mode):
        await asyncio.gather(
            cache.set("key1", "value3"),
            cache.set("key1", "value4"),
            cache.delete("key2"),
            cache.incr("key3"),
        )

    assert await cache.get("key1") in ("value4", "value3")
    assert await cache.get("key2") is None
    assert await cache.get("key3") == 1


async def test_decorators_smoke(cache: Cache, tx_mode):
    @cache.transaction(tx_mode)
    @cache.failover(ttl=10)
    @cache.cache(ttl=10)
    @cache.soft(ttl=10)
    @cache.early(ttl=10, early_ttl=5)
    @cache.hit(ttl=10, cache_hits=2)
    @cache.invalidate("key")
    @cache.slice_rate_limit(10, 10)
    @cache.circuit_breaker(10, 10, 10)
    async def do_sum(*args):
        return sum(args)

    assert await do_sum(1, 2, 5) == 8
    assert await do_sum(1, 2, 5) == 8


async def test_long_running_transaction(cache):
    @cache.transaction(TransactionMode.LOCKED, timeout=0.1)
    async def func():
        await cache.set("key", "value")
        await asyncio.sleep(1)

    with pytest.raises(LockedError):
        await asyncio.gather(func(), func())
