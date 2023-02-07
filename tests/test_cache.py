import asyncio
from typing import Optional
from unittest.mock import Mock

import pytest

from cashews import decorators, noself
from cashews.backends.memory import Memory
from cashews.formatter import _REGISTER, get_templates_for_func

pytestmark = pytest.mark.asyncio

EXPIRE = 0.02


class CustomError(Exception):
    pass


async def test_fail_cache_simple(backend):
    @decorators.failover(backend, ttl=EXPIRE, exceptions=CustomError, key="fail")
    async def func(fail=False):
        if fail:
            raise CustomError()
        return b"ok"

    assert await func() == b"ok"
    assert await func(fail=True) == b"ok"

    await asyncio.sleep(EXPIRE * 2)
    with pytest.raises(CustomError):
        await func(fail=True)


async def test_fail_cache_fast_condition(backend):
    mem = set()

    def getter(key):
        return key in mem

    def setter(key, _):
        mem.add(key)

    fast_condition = decorators.fast_condition(getter=getter, setter=setter)

    @decorators.failover(backend, ttl=EXPIRE, condition=fast_condition, key="fail", prefix="")
    async def func(fail=False):
        if fail:
            raise CustomError()
        return b"ok"

    assert await func() == b"ok"
    assert await func(fail=True) == b"ok"
    assert await func(fail=True) == b"ok"
    assert "fail" in mem


async def test_cache_simple(backend):
    @decorators.cache(backend, ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    assert await func() == b"ok"

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"

    await asyncio.sleep(EXPIRE * 1.1)
    assert await func(b"notok") == b"notok"


async def test_cache_simple_none(backend):
    mock = Mock()

    @decorators.cache(backend, ttl=EXPIRE, key="key")
    async def func():
        mock()
        return None

    assert await func() is None
    assert mock.call_count == 1

    assert await func() is None
    assert mock.call_count == 1


async def test_cache_simple_key(backend):
    _REGISTER.clear()

    @decorators.cache(backend, ttl=1, key="key:{some}")
    async def func1(resp=b"ok", some="err"):
        return resp

    @decorators.cache(backend, ttl=1)
    async def func2(resp, some="err"):
        return resp

    _noself = noself(decorators.cache)(backend, ttl=1)

    class Klass:
        @decorators.cache(backend, ttl=1)
        async def method(self, resp):
            return resp

        @staticmethod
        @decorators.cache(backend, ttl=1)
        async def stat(resp):
            return resp

        @_noself
        async def method2(self, resp):
            return resp

    await func1()
    await func2("ok")
    obj = Klass()
    await obj.method("ok")
    await obj.stat("ok")
    await obj.method2("ok")

    assert next(get_templates_for_func(func1)) == "key:{some}"
    assert next(get_templates_for_func(func2)) == "tests.test_cache:func2:resp:{resp}:some:{some}"
    assert (
        next(get_templates_for_func(obj.method))
        == "tests.test_cache:test_cache_simple_key.<locals>.Klass.method:self:{self}:resp:{resp}"
    )
    assert (
        next(get_templates_for_func(obj.method2))
        == "tests.test_cache:test_cache_simple_key.<locals>.Klass.method2:resp:{resp}"
    )
    assert next(get_templates_for_func(obj.stat)) == "tests.test_cache:stat:resp:{resp}"


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


async def test_cache_simple_ttl(backend):
    mock = Mock()

    def _ttl(resp=b"ok"):
        if resp == b"ok":
            return 0.01
        return "2h"

    @decorators.cache(backend, ttl=_ttl)
    async def func(resp=b"ok"):
        mock()
        return resp

    await func()
    await func()

    assert mock.call_count == 1
    await asyncio.sleep(0.02)

    await func()
    assert mock.call_count == 2

    await func(b"notok")
    assert mock.call_count == 3

    await asyncio.sleep(0.02)
    await func(b"notok")
    assert mock.call_count == 3


async def test_early_cache_simple(backend):
    @decorators.early(backend, ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    assert await func() == b"ok"

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"

    await asyncio.sleep(EXPIRE * 1.1)
    assert await func(b"notok") == b"notok"


async def test_soft_cache_simple(backend):
    mock = Mock()

    @decorators.soft(backend, ttl=4 * EXPIRE, soft_ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        mock()
        return resp

    assert await func() == b"ok"
    assert mock.call_count == 1

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"
    assert mock.call_count == 1

    await asyncio.sleep(EXPIRE * 1.1)
    assert await func(b"notok") == b"notok"
    assert mock.call_count == 2


async def test_soft_cache_on_exc(backend):
    mock = Mock()

    @decorators.soft(backend, ttl=4 * EXPIRE, soft_ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        if resp == b"notok":
            raise ValueError()
        mock()
        return resp

    assert await func() == b"ok"
    assert mock.call_count == 1

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"
    assert mock.call_count == 1

    await asyncio.sleep(EXPIRE * 1.1)

    assert await func(b"notok") == b"ok"
    assert mock.call_count == 1

    await asyncio.sleep(EXPIRE * 3)
    with pytest.raises(ValueError):
        await func(b"notok")


@pytest.mark.xfail
async def test_early_cache_parallel(backend):
    mock = Mock()
    await backend.init()

    @decorators.early(backend, ttl=0.1, early_ttl=0.05, key="key")
    async def func(resp=b"ok"):
        await asyncio.sleep(0.01)
        mock(resp)
        return resp

    assert await func() == b"ok"  # warm

    assert mock.call_count == 1

    for _ in range(8):
        await asyncio.sleep(0.01)
        await asyncio.gather(*[func() for _ in range(10)])

    assert mock.call_count in (2, 3, 4)  # depends on backend


async def test_lock_cache_parallel(backend):
    mock = Mock()
    await backend.init()

    @decorators.locked(backend, key="key", step=0.01)
    async def func():
        await asyncio.sleep(0.1)
        mock()

    for _ in range(2):
        await asyncio.gather(*[func() for _ in range(10)], return_exceptions=True)

    assert mock.call_count == 2


async def test_lock_cache_parallel_ttl(backend):
    mock = Mock()

    @decorators.locked(backend, key="key", step=0.01, ttl=0.1)
    async def func(resp=b"ok"):
        await asyncio.sleep(0.01)
        mock(resp)
        return resp

    for _ in range(4):
        await asyncio.gather(*[func() for _ in range(10)], return_exceptions=True)

    assert mock.call_count == 40


async def test_lock_cache_broken_backend():
    class BrokenMemoryBackend(Memory):
        async def ping(self, message: Optional[bytes] = None) -> bytes:
            raise Exception("broken")

    backend = Mock(wraps=BrokenMemoryBackend())
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

    @decorators.hit(backend, ttl=1000, cache_hits=10, key="test")
    async def func(resp=b"ok"):
        mock(resp)
        return resp

    await func()  # cache
    await asyncio.gather(*[func() for _ in range(10)])  # get 10 hits
    assert mock.call_count in [1, 2]
    await func()  # cache
    assert mock.call_count in [2, 3]

    await asyncio.gather(*[func() for _ in range(10)])
    assert mock.call_count in [2, 3, 4]


async def test_hit_cache_early(backend):
    mock = Mock()

    @decorators.hit(backend, ttl=10, cache_hits=5, key="test", update_after=1)
    async def func(resp=b"ok"):
        mock(resp)
        return resp

    assert await func(b"1") == b"1"  # nocache
    assert mock.call_count == 1

    assert await func(b"2") == b"1"  # cache and update
    assert mock.call_count == 1

    await asyncio.sleep(0.01)
    assert mock.call_count == 2
    assert await func(b"3") == b"2"  # cache from prev and also update
    await asyncio.sleep(0.01)
    assert mock.call_count == 3


async def test_context_cache_detect_simple(backend):
    @decorators.cache(backend, ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    with decorators.context_cache_detect as detector:
        assert await func() == b"ok"
        assert detector.keys == {}

        await asyncio.sleep(0)
        assert await func(b"notok") == b"ok"
        assert list(detector.keys.keys()) == [
            "key",
        ]

        await asyncio.sleep(EXPIRE * 1.1)
        assert await func(b"notok") == b"notok"
        assert len(detector.keys) == 1

    assert decorators.context_cache_detect._levels == {}


async def test_context_cache_detect_deep(backend):
    @decorators.cache(backend, ttl=EXPIRE, key="key1")
    async def func1():
        return 1

    @decorators.cache(backend, ttl=EXPIRE, key="key2")
    async def func2():
        return 2

    async def func():
        return await asyncio.gather(func1(), func2())

    with decorators.context_cache_detect as detector:
        await func()
        assert detector.keys == {}

        await asyncio.sleep(0)
        await func()

        assert len(detector.keys) == 2
        assert "key1" in detector.keys
        assert "key2" in detector.keys
    assert decorators.context_cache_detect._levels == {}


async def test_context_cache_detect_context(backend):
    @decorators.cache(backend, ttl=EXPIRE, key="key1")
    async def func1():
        return 1

    @decorators.cache(backend, ttl=EXPIRE, key="key2")
    async def func2():
        return 2

    async def func(*funcs):
        with decorators.context_cache_detect as detector:
            await asyncio.gather(*funcs)
            return len(detector.keys)

    await backend.set("key1", "test")
    await backend.set("key2", "test")
    assert await func1() == "test"
    assert await func2() == "test"

    with decorators.context_cache_detect as detector:
        assert await asyncio.create_task(func(func1())) == 1
        assert await asyncio.create_task(func(func1(), func2())) == 2
        assert await asyncio.create_task(func(func2())) == 1
        assert await asyncio.create_task(func(func2(), func2())) == 1

        assert len(detector.keys) == 2

    assert decorators.context_cache_detect._levels == {}
