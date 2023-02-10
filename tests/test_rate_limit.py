import asyncio
import uuid
from unittest.mock import Mock

import pytest

from cashews.exceptions import RateLimitError

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("n", list(range(1, 10)))
async def test_rate_limit_simple(cache, n):
    @cache.rate_limit(limit=n, period=1, prefix=str(uuid.uuid4()))
    async def func():
        return 1

    for i in range(n):
        assert await func() == 1

    with pytest.raises(RateLimitError):
        await func()


async def test_rate_limit_ttl(cache):
    @cache.rate_limit(limit=1, period=1)
    async def func():
        return 1

    assert await func() == 1

    with pytest.raises(RateLimitError):
        await func()

    await asyncio.sleep(0.01)
    with pytest.raises(RateLimitError):
        await func()

    await asyncio.sleep(1)
    await func()


async def test_rate_limit_func_args_dict(cache):
    @cache.rate_limit(limit=1, period=1, key="user:{user.name}")
    async def func(user):
        return user.name

    obj = type("user", (), {"name": "test"})()

    assert await func(obj) == "test"

    with pytest.raises(RateLimitError):
        await func(obj)

    obj = type("user", (), {"name": "new"})()
    assert await func(obj) == "new"


async def test_rate_limit_action(cache):
    action = Mock()

    @cache.rate_limit(limit=1, period=1, action=action, key="key")
    async def func(k=None):
        return 1

    assert await func() == 1
    action.assert_not_called()

    assert await func(k="test") == 1
    action.assert_called_with(k="test")


@pytest.mark.parametrize("n", list(range(1, 10)))
async def test_rate_limit_slice_simple(cache, n):
    @cache.slice_rate_limit(limit=n, period=10, prefix=str(uuid.uuid4()))
    async def func():
        return 1

    for i in range(n):
        assert await func() == 1

    with pytest.raises(RateLimitError):
        await func()


async def test_slice_rate_limit_func_args_dict(cache):
    @cache.slice_rate_limit(limit=1, period=1, key="user:{user.name}")
    async def func(user):
        return user.name

    obj = type("user", (), {"name": "test"})()

    assert await func(obj) == "test"

    with pytest.raises(RateLimitError):
        await func(obj)

    obj = type("user", (), {"name": "new"})()
    assert await func(obj) == "new"
