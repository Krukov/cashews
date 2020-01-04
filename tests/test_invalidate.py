import asyncio
import random

import pytest
from cashews import Cache
from cashews.cache_utils.simple import invalidate_func

pytestmark = pytest.mark.asyncio


@pytest.fixture(name="cache")
async def _cache():
    _cache_ = Cache()
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
    assert first_call != await func("test")


async def test_invalidate_decor_wrong(cache: Cache):
    @cache(ttl=1)
    async def func(arg, key="test"):
        return random.random()

    @cache.invalidate(func, args_map={"arg": "a"})
    async def func2(a, key="test"):
        return random.random()

    first_call = await func("test")
    await asyncio.sleep(0)

    assert first_call == await func("test")
    await func2("test2")
    await func2("test", ":")
    assert first_call == await func("test")
