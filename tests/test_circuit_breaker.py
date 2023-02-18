import asyncio

import pytest

from cashews.exceptions import CircuitBreakerOpen

pytestmark = pytest.mark.asyncio

EXPIRE = 0.02


class CustomError(Exception):
    pass


async def test_circuit_breaker_simple(cache):
    @cache.circuit_breaker(ttl=EXPIRE * 10, min_calls=10, errors_rate=5, period=1, key="test")
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


async def test_circuit_breaker_half_open(cache):
    @cache.circuit_breaker(
        ttl=EXPIRE,
        half_open_ttl=0.1,
        errors_rate=1,
        min_calls=0,
        period=1,
        key="test",
    )
    async def func(fail=False):
        if fail:
            raise CustomError()
        return b"ok"

    with pytest.raises(CustomError):
        await func(fail=True)

    with pytest.raises(CircuitBreakerOpen):
        await func(fail=True)

    await asyncio.sleep(EXPIRE)

    errors = 0
    success = 0
    for _ in range(100):
        try:
            await func(fail=False)
        except CircuitBreakerOpen:
            errors += 1
        else:
            success += 1
    assert success > 0
    assert errors > 0
    assert success + errors == 100

    await asyncio.sleep(0.1)
    assert await func() == b"ok"
