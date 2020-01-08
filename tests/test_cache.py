import asyncio
from unittest.mock import Mock

import pytest
from cashews.backends.memory import Memory
from cashews.cache_utils.early import early as early_cache
from cashews.cache_utils.fail import fail
from cashews.cache_utils.locked import locked as lock_cache
from cashews.cache_utils.rate import hit, perf
from cashews.cache_utils.simple import cache

pytestmark = pytest.mark.asyncio
EXPIRE = 0.01


class CustomError(Exception):
    pass


@pytest.fixture()
async def backend():
    _cache = Memory()
    await _cache.init()
    return _cache


async def test_fail_cache_simple(backend):
    @fail(backend, ttl=EXPIRE, exceptions=CustomError, func_args=())
    async def func(fail=False):
        if fail:
            raise CustomError()
        return b"ok"

    assert await func() == b"ok"
    await asyncio.sleep(0.001)
    assert await func(fail=True) == b"ok"

    await asyncio.sleep(EXPIRE)
    with pytest.raises(CustomError):
        await func(fail=True)


async def test_cache_simple(backend):
    @cache(backend, ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    assert await func() == b"ok"

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"

    await asyncio.sleep(EXPIRE)
    assert await func(b"notok") == b"notok"


async def test_cache_simple_enable(backend):
    @cache(backend, ttl=1, key="key", disable=lambda args: args["no_cache"])
    async def func(resp=b"ok", no_cache=False):
        return resp

    assert await func() == b"ok"

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"

    assert await func(b"notok", no_cache=True) == b"notok"


async def test_cache_simple_cond(backend):
    mock = Mock()

    @cache(backend, ttl=EXPIRE, key="key", store=lambda x: x == b"hit")
    async def func(resp=b"ok"):
        mock()
        return resp

    await func()

    assert mock.call_count == 1

    await func(b"notok")
    assert mock.call_count == 2

    await func(b"hit")
    await asyncio.sleep(0)  # allow to set cache coroutine work
    assert mock.call_count == 3
    await func(b"hit")
    assert mock.call_count == 3


async def test_early_cache_simple(backend):
    @early_cache(backend, ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    assert await func() == b"ok"

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"

    await asyncio.sleep(EXPIRE)
    assert await func(b"notok") == b"notok"


async def test_early_cache_parallel(backend):

    mock = Mock()

    @early_cache(backend, ttl=0.1, key="key")
    async def func(resp=b"ok"):
        await asyncio.sleep(0.01)
        mock(resp)
        return resp

    assert await func() == b"ok"

    assert mock.call_count == 1

    for _ in range(5):  # 0.01 (first) + 4 * 0.01 = 0.06 + 0.01(executing) 0.8 will execute
        await asyncio.sleep(0.01)
        await asyncio.gather(*[func() for _ in range(10)])

    assert mock.call_count == 1

    for _ in range(60):  # 0.01 (hit) + 60 * 0.001  = 0.07 - next hit
        await asyncio.sleep(0.001)
        await asyncio.gather(*[func() for _ in range(10)])

    assert mock.call_count in [3, 4]


async def test_lock_cache_parallel(backend):
    mock = Mock()

    @lock_cache(backend, ttl=0.1, key="key")
    async def func(resp=b"ok"):
        await asyncio.sleep(0.01)
        mock(resp)
        return resp

    for _ in range(4):
        await asyncio.sleep(0.01)
        await asyncio.gather(*[func() for _ in range(10)])

    assert mock.call_count == 1

    for _ in range(10):
        await asyncio.sleep(0.01)
        await asyncio.gather(*[func() for _ in range(10)])

    assert mock.call_count == 2


async def test_hit_cache(backend):
    mock = Mock()

    @hit(backend, ttl=10, cache_hits=10, key="test")
    async def func(resp=b"ok"):
        mock(resp)
        return resp

    await func()  # cache
    await asyncio.gather(*[func() for _ in range(10)])  # get 10 hits
    assert mock.call_count == 1
    await func()  # cache
    assert mock.call_count == 2

    await asyncio.gather(*[func() for _ in range(10)])
    assert mock.call_count == 2


async def test_perf_cache(backend):
    mock = Mock()

    @perf(backend, key="test", ttl=0.1)
    async def func(s=0.01):
        await asyncio.sleep(s)
        mock()
        return "res"

    await asyncio.gather(*[func() for _ in range(10)])
    assert mock.call_count == 10
    await func(0.015)
    assert mock.call_count == 11
    await func(0.04)  # long
    assert mock.call_count == 12

    # prev was slow so no hits
    await asyncio.gather(*[func() for _ in range(1000)])
    assert mock.call_count == 12
    await asyncio.sleep(0.07)

    await func(0.009)
    assert mock.call_count == 13
