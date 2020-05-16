import asyncio
import os
from unittest.mock import Mock

import pytest
from cashews import decorators
from cashews.backends.memory import Backend, Memory
from cashews.backends.redis import Redis

pytestmark = pytest.mark.asyncio
EXPIRE = 0.01


class CustomError(Exception):
    pass


@pytest.fixture()
async def backend():
    if os.environ.get("USE_REDIS"):
        redis = Redis("redis://", hash_key=None)
        await redis.init()
        await redis.clear()
        return redis
    _cache = Memory()
    await _cache.init()
    return _cache


async def test_fail_cache_simple(backend):
    @decorators.fail(backend, ttl=EXPIRE, exceptions=CustomError, key="fail")
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


async def test_circuit_breaker_simple(backend):
    @decorators.circuit_breaker(backend, ttl=EXPIRE * 10, errors_rate=5, period=1, key="test")
    async def func(fail=False):
        if fail:
            raise CustomError()
        return b"ok"

    for _ in range(9):
        assert await func() == b"ok"

    with pytest.raises(CustomError):
        await func(fail=True)
    await asyncio.sleep(0)

    with pytest.raises(decorators.CircuitBreakerOpen):
        await func(fail=True)

    with pytest.raises(decorators.CircuitBreakerOpen):
        await func(fail=False)


async def test_cache_simple(backend):
    @decorators.cache(backend, ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    assert await func() == b"ok"

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"

    await asyncio.sleep(EXPIRE)
    assert await func(b"notok") == b"notok"


async def test_cache_simple_none(backend):
    mock = Mock()

    @decorators.cache(backend, ttl=EXPIRE, key="key", condition=any)
    async def func():
        mock()
        return None

    assert await func() is None
    assert mock.call_count == 1

    assert await func() is None
    assert mock.call_count == 1


async def test_cache_simple_key(backend):
    @decorators.cache(backend, ttl=EXPIRE, key="key:{some}")
    async def func(resp=b"ok", some="err"):
        return resp

    @decorators.cache(backend, ttl=1, key="key2:{some}")
    async def func2(resp=b"ok", some="err"):
        return resp

    assert await func() == b"ok"
    assert await func(b"notok", some="test") == b"notok"

    assert await func(b"notok") == b"ok"

    assert await func2(b"notok") == b"notok"


async def test_cache_simple_cond(backend):
    mock = Mock()

    @decorators.cache(backend, ttl=EXPIRE, key="key", condition=lambda x, *args, **kwargs: x == b"hit")
    async def func(resp=b"ok"):
        mock()
        return resp

    await func()

    assert mock.call_count == 1

    await func(b"notok")
    assert mock.call_count == 2

    await func(b"hit")
    assert mock.call_count == 3
    await func(b"hit")
    assert mock.call_count == 3


async def test_early_cache_simple(backend):
    @decorators.early(backend, ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    assert await func() == b"ok"

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"

    await asyncio.sleep(EXPIRE)
    assert await func(b"notok") == b"notok"


async def test_early_cache_parallel(backend):

    mock = Mock()

    @decorators.early(backend, ttl=0.1, key="key")
    async def func(resp=b"ok"):
        await asyncio.sleep(0.01)
        mock(resp)
        return resp

    assert await func() == b"ok"

    assert mock.call_count == 1

    for _ in range(4):  # 0.01 (first) + 4 * 0.01 = 0.06 + 0.01(executing) 0.8 will execute
        await asyncio.sleep(0.01)
        await asyncio.gather(*[func() for _ in range(10)])

    assert mock.call_count == 1

    for _ in range(60):  # 0.01 (hit) + 60 * 0.001  = 0.07 - next hit
        await asyncio.sleep(0.001)
        await asyncio.gather(*[func() for _ in range(10)])

    assert mock.call_count in [3, 4, 5]


async def test_lock_cache_parallel(backend):
    mock = Mock()

    @decorators.locked(backend, key="key", step=0.01)
    async def func(resp=b"ok"):
        await asyncio.sleep(0.01)
        mock(resp)
        return resp

    for _ in range(4):
        await asyncio.gather(*[func() for _ in range(10)], return_exceptions=True)

    assert mock.call_count == 4


async def test_lock_cache_broken_backend():
    backend = Mock(wraps=Backend())
    mock = Mock()

    @decorators.locked(backend, key="key", step=0.01)
    async def func(resp=b"ok"):
        await asyncio.sleep(0.01)
        mock(resp)
        return resp

    for _ in range(4):
        await asyncio.gather(*[func() for _ in range(10)])

    assert mock.call_count == 40


async def test_hit_cache(backend):
    mock = Mock()

    @decorators.hit(backend, ttl=10, cache_hits=10, key="test")
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


async def test_hit_cache_early(backend):
    mock = Mock()

    @decorators.hit(backend, ttl=10, cache_hits=1, key="test", update_before=0)
    async def func(resp=b"ok"):
        mock(resp)
        return resp

    assert await func(b"1") == b"1"  # cache
    assert mock.call_count == 1

    assert await func(b"2") == b"1"  # cache
    assert mock.call_count == 1

    await asyncio.sleep(0.01)
    assert await func(b"3") == b"2"  # cache
    assert mock.call_count == 2


async def test_perf_cache(backend):
    mock = Mock()

    @decorators.perf(backend, key="test", ttl=0.2)
    async def func(s=0.01):
        await asyncio.sleep(s)
        mock()
        return "res"

    await asyncio.gather(*[func() for _ in range(10)])
    assert mock.call_count == 10
    await func(0.04)
    assert mock.call_count == 11
    with pytest.raises(decorators.PerfDegradationException):
        await func(0.001)  # long
        assert mock.call_count == 11

    # prev was slow so no hits
    with pytest.raises(decorators.PerfDegradationException):
        await asyncio.gather(*[func() for _ in range(1000)])

    assert mock.call_count == 11
    await asyncio.sleep(0.2)

    await func(0.009)
    assert mock.call_count == 12


async def test_cache_detect_simple(backend):
    @decorators.cache(backend, ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    cache_detect = decorators.CacheDetect()
    assert await func(_from_cache=cache_detect) == b"ok"
    assert cache_detect.get() == {}

    await asyncio.sleep(0)
    assert await func(b"notok", _from_cache=cache_detect) == b"ok"
    assert len(cache_detect.get()) == 1
    assert list(cache_detect.get().keys()) == [
        f"key:{EXPIRE}",
    ]


async def test_context_cache_detect_simple(backend):
    @decorators.cache(backend, ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    decorators.context_cache_detect.start()
    assert await func() == b"ok"
    assert decorators.context_cache_detect.get() == {}

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"
    assert len(decorators.context_cache_detect.get()) == 1
    assert list(decorators.context_cache_detect.get().keys()) == [
        f"key:{EXPIRE}",
    ]

    await asyncio.sleep(EXPIRE)
    assert await func(b"notok") == b"notok"
    assert len(decorators.context_cache_detect.get()) == 1


async def test_context_cache_detect_deep(backend):
    @decorators.cache(backend, ttl=EXPIRE, key="key1")
    async def func1():
        return 1

    @decorators.cache(backend, ttl=EXPIRE, key="key2")
    async def func2():
        return 2

    async def func():
        return await asyncio.gather(func1(), func2())

    decorators.context_cache_detect.start()
    await func()
    assert decorators.context_cache_detect.get() == {}

    await asyncio.sleep(0)
    await func()

    assert len(decorators.context_cache_detect.get()) == 2
    assert f"key1:{EXPIRE}" in decorators.context_cache_detect.get()
    assert f"key2:{EXPIRE}" in decorators.context_cache_detect.get()


async def test_context_cache_detect_context(backend):
    assert decorators.context_cache_detect.get() is None

    @decorators.cache(backend, ttl=EXPIRE, key="key1")
    async def func1():
        return 1

    @decorators.cache(backend, ttl=EXPIRE, key="key2")
    async def func2():
        return 2

    async def func(*funcs):
        decorators.context_cache_detect.start()
        res = await asyncio.gather(*funcs)
        return len(decorators.context_cache_detect.get())

    await backend.set(f"key1:{EXPIRE}", "test")
    await backend.set(f"key2:{EXPIRE}", "test")
    assert await func1() == "test"
    assert await func2() == "test"

    assert decorators.context_cache_detect.get() is None

    assert await asyncio.create_task(func(func1())) == 1
    assert await asyncio.create_task(func(func1(), func2())) == 2
    assert await asyncio.create_task(func(func2(), func2())) == 1
    assert decorators.context_cache_detect.get() is None  # ! It should be 3 but for now  it is ok, have no user case
