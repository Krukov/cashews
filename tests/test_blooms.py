from unittest.mock import Mock

import pytest

from cashews.decorators.bloom import bloom, dual_bloom

pytestmark = pytest.mark.asyncio


async def test_bloom_simple(cache):
    n = 100
    call = Mock()

    @bloom(backend=cache, name="name:{k}", false_positives=1, capacity=n)
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
    if backend.__class__.__name__ == "DiskCache":
        pytest.skip()

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

    @dual_bloom(backend=cache, name="name:{k}", false=1, capacity=n)
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
