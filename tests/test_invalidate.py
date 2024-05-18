import asyncio
import random

from cashews import Cache
from cashews.validation import invalidate_further


async def test_invalidate_decor_str(cache: Cache):
    @cache(ttl=1, key="key:{arg}")
    async def func(arg):
        return random.random()

    @cache.invalidate("key:{arg}")
    async def func2(arg, default=True):
        return random.random()

    first_call = await func("test")
    await asyncio.sleep(0.01)

    assert first_call == await func("test")
    await func2("test")
    await asyncio.sleep(0.01)
    assert first_call != await func("test")


async def test_invalidate_decor_tag(cache: Cache):
    @cache(ttl=1, key="key:{arg}", tags=("key:{arg}", "key"))
    async def func(arg):
        return random.random()

    @cache.invalidate("key:{arg}")
    async def func2(arg, default=True):
        return random.random()

    first_call = await func("test")
    await asyncio.sleep(0.01)

    assert first_call == await func("test")
    await func2("test")
    await asyncio.sleep(0.01)
    assert first_call != await func("test")


async def test_invalidate_decor_tag_func(cache: Cache):
    @cache(ttl=1, key="key:{arg:hash}", tags=("key:{arg}", "key"))
    async def func(arg):
        return random.random()

    @cache.invalidate("key:{arg:hash}")
    async def func2(arg, default=True):
        return random.random()

    first_call = await func("test")
    await asyncio.sleep(0.01)

    assert first_call == await func("test")
    await func2("test")
    await asyncio.sleep(0.01)
    assert first_call != await func("test")


async def test_invalidate_further_decorator(cache):
    @cache(ttl=100)
    async def func():
        return random.random()

    first = await func()
    assert await func() == first

    with invalidate_further():
        on_clear = await func()
    assert on_clear is not None
    assert on_clear != first  # the key was deleted
    second = await func()
    assert second != first
    assert await func() == second


async def test_invalidate_further_get(cache):
    await cache.set("key", "value")
    assert await cache.get("key") == "value"

    with invalidate_further():
        assert await cache.set("key", "value2")
        assert await cache.set("key2", "value2")
        assert await cache.get("key") is None

    await asyncio.sleep(0.01)
    assert await cache.get("key") is None
    assert await cache.get("key2") == "value2"
    assert await cache.set("key", "value3")
    assert await cache.get("key") == "value3"


async def test_invalidate_further_get_many(cache):
    await cache.set("key", "value")
    await cache.set("key2", "value2")

    assert await cache.get_many("key", "key2") == ("value", "value2")

    with invalidate_further():
        assert await cache.get_many("key") == (None,)

    await asyncio.sleep(0.01)
    assert await cache.get_many("key", "key2") == (None, "value2")


async def test_invalidate_further_get_match(cache):
    await cache.set("key1", "value1")
    await cache.set("key2", "value2")
    assert {k: v async for k, v in cache.get_match("key*")} == {
        "key1": "value1",
        "key2": "value2",
    }

    with invalidate_further():
        assert {k: v async for k, v in cache.get_match("key1*")} == {}

    await asyncio.sleep(0.01)
    assert {k: v async for k, v in cache.get_match("key*")} == {"key2": "value2"}
