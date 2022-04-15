import asyncio
import random

import pytest

from cashews import Cache
from cashews.validation import invalidate_func, set_invalidate_further

pytestmark = pytest.mark.asyncio

_cache_ = Cache()


@pytest.fixture(name="cache")
async def _cache():
    _cache_.setup("mem://")
    return _cache_


async def test_invalidate_func(cache: Cache):
    @cache(ttl=1)
    async def func(arg):
        return random.random()

    first_call = await func("test")
    await asyncio.sleep(0)

    assert first_call == await func("test")
    await invalidate_func(cache, func, kwargs={"arg": "test"})
    assert first_call != await func("test")


async def test_invalidate_func_wrong(cache: Cache):
    @cache(ttl=1)
    async def func(arg):
        return random.random()

    first_call = await func("test")
    await asyncio.sleep(0)

    assert first_call == await func("test")
    await invalidate_func(cache, func, kwargs={"arg": "test2"})
    assert first_call == await func("test")


async def test_invalidate_decor(cache: Cache):
    @cache(ttl=1)
    async def func(arg):
        return random.random()

    @cache.invalidate(func, args_map={"arg": "a"})
    async def func2(a):
        return random.random()

    first_call = await func("test")
    await asyncio.sleep(0)

    assert first_call == await func("test")
    await func2("test")
    await asyncio.sleep(0)
    assert first_call != await func("test")


async def test_invalidate_decor_str(cache: Cache):
    @cache(ttl=1, key="key:{arg}")
    async def func(arg):
        return random.random()

    @cache.invalidate("key:{arg}")
    async def func2(arg, default=True):
        return random.random()

    first_call = await func("test")
    await asyncio.sleep(0)

    assert first_call == await func("test")
    await func2("test")
    await asyncio.sleep(0)
    assert first_call != await func("test")


async def test_invalidate_decor_complicate(cache: Cache):
    @cache(ttl=1, key="arg-{arg}:{key}:{flag}")
    async def func(arg, key=b"test", flag=True, **kwargs):
        return random.random()

    @cache.invalidate(func, args_map={"arg": "a"}, defaults={"flag": True})
    async def func2(a):
        return random.random()

    first_call = await func("test")
    not_invalidate = await func("test", flag=False)
    second_call = await func("test", b"key", flag=True)
    last_call = await func("test2", None)
    await asyncio.sleep(0)

    assert first_call != second_call != last_call
    assert first_call == await func("test")
    assert second_call == await func("test", b"key")
    assert last_call == await func("test2", None)
    assert not_invalidate == await func("test", flag=False)

    await func2("test3")

    await asyncio.sleep(0)
    assert first_call == await func("test")
    assert second_call == await func("test", b"key")
    assert last_call == await func("test2", None)
    assert not_invalidate == await func("test", flag=False)

    await func2("test")

    await asyncio.sleep(0)
    assert first_call != await func("test")
    assert second_call != await func("test", "key")
    assert last_call == await func("test2", None)
    assert not_invalidate == await func("test", flag=False)

    await func2("test2")

    await asyncio.sleep(0)
    assert first_call != await func("test")
    assert not_invalidate == await func("test", flag=False)


async def test_invalidate_further(cache):
    @cache(ttl=10000)
    async def func():
        return random.random()

    async def clear():
        set_invalidate_further()
        return await func()

    first = await func()
    assert await func() == first

    assert await asyncio.create_task(clear()) != first  # the key was deleted
    second = await func()
    assert second != first
    assert await func() == second
