from unittest.mock import Mock

import pytest

from cashews.decorators.bloom import bloom

pytestmark = pytest.mark.asyncio


async def test_bloom_simple(cache):
    n = 100
    call = Mock()

    @cache.bloom(name="name:{k}", false_positives=1, capacity=n)
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


async def test_bloom_dual(cache):
    n = 100
    call = Mock()

    @cache.dual_bloom(name="name:{k}", false=1, capacity=n)
    async def func(k):
        call(k)
        return k > (n / 2)

    for i in range(n - 1):
        await func(i)

    call.reset_mock()
    for i in range(n - 1):
        assert await func(i) is (i > (n / 2))

    assert call.call_count == 0

    assert await func(100) is True
    call.assert_called_with(100)
