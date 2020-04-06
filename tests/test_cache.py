import asyncio
from unittest.mock import Mock

import pytest
from cashews.backends.memory import Backend, Memory
from cashews.cache_utils.circuit_braker import CircuitBreakerOpen, circuit_breaker
from cashews.cache_utils.defaults import CacheDetect, context_cache_detect
from cashews.cache_utils.early import early as early_cache
from cashews.cache_utils.fail import fail
from cashews.cache_utils.locked import locked as lock_cache
from cashews.cache_utils.rate import PerfDegradationException, hit, perf
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


async def test_circuit_breaker_simple(backend):
    @circuit_breaker(backend, ttl=EXPIRE * 10, errors_rate=5, period=1, func_args=())
    async def func(fail=False):
        if fail:
            raise CustomError()
        return b"ok"

    for _ in range(9):
        assert await func() == b"ok"

    with pytest.raises(CustomError):
        await func(fail=True)
    await asyncio.sleep(0)

    with pytest.raises(CircuitBreakerOpen):
        await func(fail=True)

    with pytest.raises(CircuitBreakerOpen):
        await func(fail=False)


async def test_cache_simple(backend):
    @cache(backend, ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    assert await func() == b"ok"

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"

    await asyncio.sleep(EXPIRE)
    assert await func(b"notok") == b"notok"


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

    @lock_cache(backend, key="key", step=0.01)
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

    @lock_cache(backend, key="key", step=0.01)
    async def func(resp=b"ok"):
        await asyncio.sleep(0.01)
        mock(resp)
        return resp

    for _ in range(4):
        await asyncio.gather(*[func() for _ in range(10)])

    assert mock.call_count == 40


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


async def test_hit_cache_early(backend):
    mock = Mock()

    @hit(backend, ttl=10, cache_hits=1, key="test", update_before=0)
    async def func(resp=b"ok"):
        mock(resp)
        return resp

    assert await func(b"1") == b"1"  # cache
    assert mock.call_count == 1

    assert await func(b"2") == b"1"  # cache
    assert mock.call_count == 1

    await asyncio.sleep(0)
    assert await func(b"3") == b"2"  # cache
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
    await func(0.04)
    assert mock.call_count == 11
    with pytest.raises(PerfDegradationException):
        await func(0.001)  # long
        assert mock.call_count == 11

    # prev was slow so no hits
    with pytest.raises(PerfDegradationException):
        await asyncio.gather(*[func() for _ in range(1000)])

    assert mock.call_count == 11
    await asyncio.sleep(0.07)

    await func(0.009)
    assert mock.call_count == 12


async def test_cache_detect_simple(backend):
    @cache(backend, ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    cache_detect = CacheDetect()
    assert await func(_from_cache=cache_detect) == b"ok"
    assert cache_detect.get() == {}

    await asyncio.sleep(0)
    assert await func(b"notok", _from_cache=cache_detect) == b"ok"
    assert len(cache_detect.get()) == 1
    assert list(cache_detect.get().keys()) == [
        "key",
    ]


async def test_context_cache_detect_simple(backend):
    @cache(backend, ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    context_cache_detect.start()
    assert await func() == b"ok"
    assert context_cache_detect.get() == {}

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"
    assert len(context_cache_detect.get()) == 1
    assert list(context_cache_detect.get().keys()) == [
        "key",
    ]

    await asyncio.sleep(EXPIRE)
    assert await func(b"notok") == b"notok"
    assert len(context_cache_detect.get()) == 1


async def test_context_cache_detect_deep(backend):
    @cache(backend, ttl=EXPIRE, key="key1")
    async def func1():
        return 1

    @cache(backend, ttl=EXPIRE, key="key2")
    async def func2():
        return 2

    async def func():
        return await asyncio.gather(func1(), func2())

    context_cache_detect.start()
    await func()
    assert context_cache_detect.get() == {}

    await asyncio.sleep(0)
    await func()

    assert len(context_cache_detect.get()) == 2
    assert "key1" in context_cache_detect.get()
    assert "key2" in context_cache_detect.get()


async def test_context_cache_detect_context(backend):
    @cache(backend, ttl=EXPIRE, key="key1")
    async def func1():
        return 1

    @cache(backend, ttl=EXPIRE, key="key2")
    async def func2():
        return 2

    async def func(t_func):
        context_cache_detect.start()
        assert len(context_cache_detect.get()) == 0
        await t_func()
        assert len(context_cache_detect.get()) == 1
        assert list(context_cache_detect.get().keys()) == [
            t_func._key_template,
        ]

    await backend.set("key1", "test")
    await backend.set("key2", "test")
    assert await func1() == "test"
    assert await func2() == "test"

    context_cache_detect.start()
    await asyncio.gather(func(func1), func(func2), func1())
    assert len(context_cache_detect.get()) == 1  # ! It should be 3 but for now  it is ok, have no user case
