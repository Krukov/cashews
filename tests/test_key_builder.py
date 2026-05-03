import asyncio
import json
from hashlib import sha256
from unittest.mock import Mock

import pytest

from cashews import Cache
from cashews.exceptions import RateLimitError

EXPIRE = 0.1


def products_key_builder(func, args, kwargs):
    return f"products:{kwargs.get('shop_id', 'unknown')}"


def hash_key_builder(func, args, kwargs):
    data = {k: v for k, v in kwargs.items() if k != "session"}
    payload = json.dumps(data, sort_keys=True, default=str)
    return "cache:" + sha256(payload.encode()).hexdigest()[:16]


def args_key_builder(func, args, kwargs):
    return f"key:{args[0]}"


async def test_cache_key_builder_simple(cache: Cache):
    mock = Mock()

    @cache(ttl=EXPIRE, key_builder=args_key_builder)
    async def func(x, y=1):
        mock(x, y)
        return x + y

    assert await func(1, 2) == 3
    assert mock.call_count == 1

    assert await func(1, 99) == 3
    assert mock.call_count == 1

    await asyncio.sleep(EXPIRE * 1.2)
    assert await func(1, 2) == 3
    assert mock.call_count == 2


async def test_cache_key_builder_different_keys(cache: Cache):
    mock = Mock()

    @cache(ttl=EXPIRE, key_builder=args_key_builder)
    async def func(x):
        mock(x)
        return x * 2

    assert await func(1) == 2
    assert await func(2) == 4
    assert mock.call_count == 2

    assert await func(1) == 2
    assert await func(2) == 4
    assert mock.call_count == 2


async def test_cache_key_builder_exclude_session(cache: Cache):
    mock = Mock()

    @cache(ttl=EXPIRE, key_builder=products_key_builder)
    async def get_products(session, shop_id, page=1):
        mock(shop_id, page)
        return {"products": [], "shop_id": shop_id, "page": page}

    assert await get_products(session="sess1", shop_id=42, page=1) == {
        "products": [],
        "shop_id": 42,
        "page": 1,
    }
    assert mock.call_count == 1

    assert await get_products(session="sess2", shop_id=42, page=1) == {
        "products": [],
        "shop_id": 42,
        "page": 1,
    }
    assert mock.call_count == 1

    assert await get_products(session="sess3", shop_id=99, page=1) == {
        "products": [],
        "shop_id": 99,
        "page": 1,
    }
    assert mock.call_count == 2


async def test_cache_key_builder_hash_normalization(cache: Cache):
    mock = Mock()

    @cache(ttl=EXPIRE, key_builder=hash_key_builder)
    async def func(session, filters):
        mock(session, filters)
        return filters

    result1 = await func(session="s1", filters={"a": 1, "b": 2})
    result2 = await func(session="s2", filters={"a": 1, "b": 2})
    assert result1 == result2
    assert mock.call_count == 1

    result3 = await func(session="s3", filters={"a": 1, "b": 3})
    assert result3 == {"a": 1, "b": 3}
    assert mock.call_count == 2


async def test_cache_key_and_key_builder_exclusive(cache: Cache):
    with pytest.raises(ValueError, match="cannot be used together"):

        @cache(ttl=EXPIRE, key="mykey:{x}", key_builder=args_key_builder)
        async def func(x):
            return x


async def test_failover_key_builder(cache: Cache):
    class CustomError(Exception):
        pass

    mock = Mock()

    @cache.failover(ttl=EXPIRE, exceptions=CustomError, key_builder=args_key_builder)
    async def func(x, fail=False):
        mock(x)
        if fail:
            raise CustomError()
        return x

    assert await func(1) == 1
    assert mock.call_count == 1

    assert await func(1, fail=True) == 1
    assert mock.call_count == 2

    await asyncio.sleep(EXPIRE * 2)
    with pytest.raises(CustomError):
        await func(1, fail=True)


async def test_early_key_builder(cache: Cache):
    mock = Mock()

    @cache.early(ttl=EXPIRE, early_ttl=0.05, key_builder=args_key_builder, background=False)
    async def func(x):
        mock(x)
        return x

    assert await func(1) == 1
    assert mock.call_count == 1

    assert await func(1) == 1
    assert mock.call_count == 1

    await asyncio.sleep(EXPIRE * 1.5)
    assert await func(1) == 1
    assert mock.call_count == 2


async def test_soft_key_builder(cache: Cache):
    mock = Mock()

    @cache.soft(ttl=4 * EXPIRE, soft_ttl=EXPIRE, key_builder=args_key_builder)
    async def func(x):
        mock(x)
        return x

    assert await func(1) == 1
    assert mock.call_count == 1

    await asyncio.sleep(0)
    assert await func(1) == 1
    assert mock.call_count == 1

    await asyncio.sleep(EXPIRE * 1.2)
    assert await func(1) == 1
    assert mock.call_count == 2


async def test_hit_key_builder(cache: Cache):
    mock = Mock()

    @cache.hit(ttl=1000, cache_hits=10, key_builder=args_key_builder)
    async def func(x):
        mock(x)
        return x

    await func(1)
    await asyncio.gather(*[func(1) for _ in range(10)])
    assert mock.call_count <= 2


async def test_rate_limit_key_builder(cache: Cache):
    @cache.rate_limit(limit=2, period=EXPIRE, key_builder=args_key_builder)
    async def func(x):
        return x

    assert await func(1) == 1
    assert await func(1) == 1

    with pytest.raises(RateLimitError):
        await func(1)

    assert await func(2) == 2


async def test_locked_key_builder(cache: Cache):
    order = []

    @cache.locked(ttl=EXPIRE, key_builder=args_key_builder)
    async def func(x):
        order.append("start")
        await asyncio.sleep(0.01)
        order.append("end")
        return x

    results = await asyncio.gather(func(1), func(1))
    assert results == [1, 1]
    assert order == ["start", "end", "start", "end"]


async def test_cache_key_builder_with_condition(cache: Cache):
    mock = Mock()

    def _condition(result, args, kwargs, key=""):
        return result != "skip"

    @cache(ttl=EXPIRE, key_builder=args_key_builder, condition=_condition)
    async def func(x):
        mock(x)
        return x

    assert await func("skip") == "skip"
    assert mock.call_count == 1

    assert await func("skip") == "skip"
    assert mock.call_count == 2

    assert await func("cache_me") == "cache_me"
    assert mock.call_count == 3

    assert await func("cache_me") == "cache_me"
    assert mock.call_count == 3


async def test_cache_key_builder_with_lock(cache: Cache):
    order = []

    @cache(ttl=EXPIRE, lock=True, key_builder=args_key_builder)
    async def func(x):
        order.append(f"start-{x}")
        await asyncio.sleep(0.01)
        order.append(f"end-{x}")
        return x

    results = await asyncio.gather(func(1), func(1))
    assert results == [1, 1]
    assert order.index("start-1") < order.index("end-1")


async def test_dynamic_key_builder(cache: Cache):
    mock = Mock()

    @cache.dynamic(ttl=1000, key_builder=args_key_builder)
    async def func(x):
        mock(x)
        return x

    assert await func(1) == 1
    assert mock.call_count == 1

    assert await func(1) == 1
    assert mock.call_count == 1


async def test_circuit_breaker_key_builder(cache: Cache):
    class CustomError(Exception):
        pass

    @cache.circuit_breaker(
        errors_rate=90, period=EXPIRE, ttl=EXPIRE, exceptions=CustomError, key_builder=args_key_builder
    )
    async def func(x, fail=False):
        if fail:
            raise CustomError()
        return x

    assert await func(1) == 1

    with pytest.raises(CustomError):
        await func(1, fail=True)


async def test_slice_rate_limit_key_builder(cache: Cache):
    @cache.slice_rate_limit(limit=2, period=EXPIRE, key_builder=args_key_builder)
    async def func(x):
        return x

    assert await func(1) == 1
    assert await func(1) == 1

    with pytest.raises(RateLimitError):
        await func(1)

    assert await func(2) == 2
