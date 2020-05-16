import asyncio
import uuid
from functools import partial
from operator import attrgetter
from unittest.mock import Mock

import pytest
from cashews.backends.memory import Memory
from cashews.decorators.rate import RateLimitException
from cashews.decorators.rate import rate_limit as _rate_limit

pytestmark = pytest.mark.asyncio


@pytest.fixture()
async def rate_limit():
    return partial(_rate_limit, backend=Memory())


@pytest.mark.parametrize("n", list(range(1, 10)))
async def test_rate_limit_simple(rate_limit, n):
    @rate_limit(limit=n, period=0.01, prefix=str(uuid.uuid4()))
    async def func():
        return 1

    for i in range(n):
        assert await func() == 1

    with pytest.raises(RateLimitException):
        await func()

    await asyncio.sleep(0.01)
    assert await func() == 1


async def test_rate_limit_ttl(rate_limit):
    @rate_limit(limit=1, period=0.02)
    async def func():
        return 1

    assert await func() == 1

    with pytest.raises(RateLimitException):
        await func()

    await asyncio.sleep(0.01)
    with pytest.raises(RateLimitException):
        await func()

    await asyncio.sleep(0.01)
    await func()


async def test_rate_limit_func_args_list(rate_limit):
    @rate_limit(limit=1, period=0.01, key="{arg1}:{kwarg2}", prefix=str(uuid.uuid4()))
    async def func(*args, **kwargs):
        return len(args) + len(kwargs)

    assert await func() == 0

    with pytest.raises(RateLimitException):
        await func()

    assert await func(arg1="test") == 1
    with pytest.raises(RateLimitException):
        await func(arg1="test")

    assert await func(arg1="test2") == 1

    assert await func(arg1="test", kwarg2="test") == 2
    assert await func(arg1="test2", kwarg2="test2") == 2

    with pytest.raises(RateLimitException):
        await func(arg1="test2", kwarg2="test2")


async def test_rate_limit_func_args_dict(rate_limit):
    @rate_limit(limit=1, period=0.1, key="user:{user.name}")
    async def func(user):
        return user.name

    obj = type("user", (), {"name": "test"})()

    assert await func(obj) == "test"

    with pytest.raises(RateLimitException):
        await func(obj)

    obj.name = "new"
    assert await func(obj) == "new"


async def test_rate_limit_action(rate_limit):
    action = Mock()

    @rate_limit(limit=1, period=0.1, action=action, key="key")
    async def func(k=None):
        return 1

    assert await func() == 1
    action.assert_not_called()

    assert await func(k="test") == 1
    action.assert_called_with(k="test")
