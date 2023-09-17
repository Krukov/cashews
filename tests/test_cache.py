import asyncio
from unittest.mock import Mock

import pytest

from cashews import Cache, decorators, noself
from cashews.formatter import _REGISTER, get_templates_for_func

pytestmark = pytest.mark.asyncio

EXPIRE = 0.02


class CustomError(Exception):
    pass


async def test_fail_cache_simple(cache: Cache):
    @cache.failover(ttl=EXPIRE, exceptions=CustomError, key="fail")
    async def func(fail=False):
        if fail:
            raise CustomError()
        return b"ok"

    assert await func() == b"ok"
    assert await func(fail=True) == b"ok"

    await asyncio.sleep(EXPIRE * 2)
    with pytest.raises(CustomError):
        await func(fail=True)


async def test_fail_cache_fast_condition(cache: Cache):
    mem = set()

    def getter(key):
        return key in mem

    def setter(key, _):
        mem.add(key)

    fast_condition = decorators.fast_condition(getter=getter, setter=setter)

    @cache.failover(ttl=EXPIRE, condition=fast_condition, key="fail", prefix="")
    async def func(fail=False):
        if fail:
            raise CustomError()
        return b"ok"

    assert await func() == b"ok"
    assert await func(fail=True) == b"ok"
    assert await func(fail=True) == b"ok"
    assert "fail" in mem


async def test_cache_simple(cache: Cache):
    @cache.cache(ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    assert await func() == b"ok"

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"

    await asyncio.sleep(EXPIRE * 1.1)
    assert await func(b"notok") == b"notok"


async def test_cache_simple_none(cache: Cache):
    mock = Mock()

    @cache(ttl=EXPIRE, key="key")
    async def func():
        mock()
        return None

    assert await func() is None
    assert mock.call_count == 1

    assert await func() is None
    assert mock.call_count == 1


async def test_cache_simple_key(cache: Cache):
    _REGISTER.clear()

    @cache(ttl=1, key="key:{some}")
    async def func1(resp=b"ok", some="err"):
        return resp

    @cache(ttl=1)
    async def func2(resp, some="err"):
        return resp

    await func1()
    await func2("ok")

    assert next(get_templates_for_func(func1)) == "key:{some}"
    assert next(get_templates_for_func(func2)) == "tests.test_cache:func2:resp:{resp}:some:{some}"


async def test_cache_self_noself_key(cache: Cache):
    _REGISTER.clear()

    _noself = noself(cache)

    class Klass:
        @cache(ttl=1)
        async def method(self, resp):
            return resp

        @staticmethod
        @cache(ttl=1)
        async def stat(resp):
            return resp

        @_noself(ttl=1)
        async def method2(self, resp):
            return resp

    obj = Klass()
    await obj.method("ok")
    await obj.stat("ok")
    await obj.method2("ok")

    assert (
        next(get_templates_for_func(obj.method))
        == "tests.test_cache:test_cache_self_noself_key.<locals>.Klass.method:self:{self}:resp:{resp}"
    )
    assert (
        next(get_templates_for_func(obj.method2))
        == "tests.test_cache:test_cache_self_noself_key.<locals>.Klass.method2:resp:{resp}"
    )
    assert next(get_templates_for_func(obj.stat)) == "tests.test_cache:stat:resp:{resp}"


async def test_cache_simple_cond(cache: Cache):
    mock = Mock()

    @cache(ttl=EXPIRE, key="key", condition=lambda x, *args, **kwargs: x == b"hit")
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


async def test_cache_simple_ttl(cache: Cache):
    mock = Mock()

    def _ttl(resp=b"ok"):
        if resp == b"ok":
            return 0.01
        return "2h"

    @cache(ttl=_ttl)
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


async def test_cache_simple_ttl_with_result(cache: Cache):
    mock = Mock()

    def _ttl(result, resp=b"ok"):
        if result == b"ok":
            return 0.01
        return "2h"

    @cache(ttl=_ttl)
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
    await func(resp=b"notok")
    assert mock.call_count == 3

    await asyncio.sleep(0.02)
    await func(b"notok")
    assert mock.call_count == 3


async def test_early_cache_simple(cache: Cache):
    @cache.early(ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    assert await func() == b"ok"

    await asyncio.sleep(0)
    assert await func(b"notok") == b"ok"

    await asyncio.sleep(EXPIRE * 1.1)
    assert await func(b"notok") == b"notok"


async def test_soft_cache_simple(cache: Cache):
    mock = Mock()

    @cache.soft(ttl=4 * EXPIRE, soft_ttl=EXPIRE, key="key")
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


async def test_soft_cache_on_exc(cache: Cache):
    mock = Mock()

    @cache.soft(ttl=4 * EXPIRE, soft_ttl=EXPIRE, key="key")
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
async def test_early_cache_parallel(cache: Cache):
    mock = Mock()

    @cache.early(ttl=0.1, early_ttl=0.05, key="key")
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


async def test_hit_cache(cache: Cache):
    mock = Mock()

    @cache.hit(ttl=1000, cache_hits=10, key="test")
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


async def test_hit_cache_early(cache: Cache):
    mock = Mock()

    @cache.hit(ttl=10, cache_hits=5, key="test", update_after=1)
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


async def test_context_cache_detect_simple(cache: Cache):
    @cache(ttl=EXPIRE, key="key")
    async def func(resp=b"ok"):
        return resp

    with decorators.context_cache_detect as detector:
        assert await func() == b"ok"
        assert detector.calls == {}

        await asyncio.sleep(0)
        assert await func(b"notok") == b"ok"
        assert list(detector.calls.keys()) == [
            "key",
        ]

        await asyncio.sleep(EXPIRE * 1.1)
        assert await func(b"notok") == b"notok"
        assert len(detector.calls) == 1

    assert decorators.context_cache_detect._levels == {}


async def test_context_cache_detect_deep(cache: Cache):
    @cache(ttl=EXPIRE, key="key1")
    async def func1():
        return 1

    @cache(ttl=EXPIRE, key="key2")
    async def func2():
        return 2

    async def func():
        return await asyncio.gather(func1(), func2())

    with decorators.context_cache_detect as detector:
        await func()
        assert detector.calls == {}

        await asyncio.sleep(0)
        await func()

        assert len(detector.calls) == 2
        assert "key1" in detector.calls
        assert "key2" in detector.calls
    assert decorators.context_cache_detect._levels == {}


async def test_context_cache_detect_context(cache: Cache):
    @cache(ttl=EXPIRE, key="key1")
    async def func1():
        return 1

    @cache(ttl=EXPIRE, key="key2")
    async def func2():
        return 2

    async def func(*funcs):
        with decorators.context_cache_detect as detector:
            await asyncio.gather(*funcs)
            return len(detector.calls)

    await cache.set("key1", "test")
    await cache.set("key2", "test")
    assert await func1() == "test"
    assert await func2() == "test"

    with decorators.context_cache_detect as detector:
        assert await asyncio.create_task(func(func1())) == 1
        assert await asyncio.create_task(func(func1(), func2())) == 2
        assert await asyncio.create_task(func(func2())) == 1
        assert await asyncio.create_task(func(func2(), func2())) == 1

        assert len(detector.calls) == 2

    assert decorators.context_cache_detect._levels == {}
