from unittest.mock import Mock

import pytest

from cashews.backends.memory import Memory
from cashews.decorators.bloom import _counting_bloom as counting_bloom
from cashews.decorators.bloom import bloom, dual_bloom

pytestmark = pytest.mark.asyncio


@pytest.fixture(
    name="backend",
    params=[
        "memory",
        pytest.param("redis", marks=pytest.mark.redis),
        pytest.param("diskcache", marks=pytest.mark.diskcache),
    ],
)
async def _cache(request, redis_dsn, backend_factory):
    if request.param == "diskcache":
        from cashews.backends.diskcache import DiskCache

        backend = await backend_factory(DiskCache, shards=0)
        yield backend
    elif request.param == "redis":
        from cashews.backends.redis import Redis

        yield await backend_factory(Redis, redis_dsn, hash_key=None)
    else:
        yield await backend_factory(Memory)


async def test_bloom_simple(backend):
    n = 100
    call = Mock()

    @bloom(backend=backend, name="name:{k}", false_positives=1, capacity=n)
    async def func(k):
        call(k)
        return k > (n / 2)

    for i in range(n):
        await func.set(i)

    for i in range(n):
        call.reset_mock()
        assert await func(i) is (i > (n / 2))
        if i > (n / 2):
            call.assert_called_with(i)
        else:
            call.assert_not_called()


async def test_bloom_simple_big_size(backend):
    n = 1_000_000
    if backend.name == "diskcache":
        n = 10_000
    call = Mock()

    @bloom(backend=backend, name="name:{k}", false_positives=1, capacity=n)
    async def func(k):
        call(k)
        return k > (n / 2)

    await func.set(900_000)
    await func.set(1_000)

    call.reset_mock()
    assert await func(900_000)
    call.assert_called_with(900_000)

    call.reset_mock()
    assert not await func(1_000)
    call.assert_not_called()


async def test_bloom_dual(backend):
    n = 100
    call = Mock()

    @dual_bloom(backend=backend, name="name:{k}", false=1, capacity=n)
    async def func(k):
        call(k)
        return k > (n / 2)

    for i in range(n):
        await func(i)

    call.reset_mock()
    for i in range(n):
        assert await func(i) is (i > (n / 2))
        call.assert_not_called()

    await func.delete(10)
    await func.delete(70)

    assert await func(10) is False
    call.assert_called_with(10)

    call.reset_mock()
    assert await func(70) is True
    call.assert_called_with(70)


async def test_bloom_counting(backend):
    n = 100
    call = Mock()

    @counting_bloom(backend=backend, name="name:{k}", false_positives=0.1, capacity=n)
    async def func(k):
        call(k)
        return k > (n / 2)

    for i in range(n):
        await func.set(i)

    call.reset_mock()
    for i in range(n):
        assert await func(i) is (i > (n / 2))

    await func.delete(10)
    await func.delete(70)

    assert await func(10) is False
    call.assert_called_with(10)

    call.reset_mock()
    assert await func(70) is True
    call.assert_called_with(70)
