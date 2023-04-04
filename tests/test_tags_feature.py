import asyncio
from random import random
from unittest.mock import AsyncMock

import pytest

from cashews import Cache

pytestmark = pytest.mark.asyncio


def test_register_tags(cache: Cache):
    cache.register_tag("tag", "key")
    cache.register_tag("tag_template", "key{i}")
    cache.register_tag("tag_test:{i}", "key{i}:test")

    assert cache.get_key_tags("key1:test") == ["tag_template", "tag_test:1"]
    assert cache.get_key_tags("keyanytest") == ["tag_template"]
    assert cache.get_key_tags("keytest") == ["tag_template"]
    assert cache.get_key_tags("key") == ["tag", "tag_template"]
    assert cache.get_key_tags("k") == []
    assert cache.get_key_tags("prefixkey") == []
    assert cache.get_key_tags(":key") == []


async def test_tags_no_delete(cache: Cache):
    cache.register_tag("3", "key3")
    await cache.set("key3", "value", tags=["3"])
    await cache.delete_tags("2")
    assert await cache.get("key3") == "value"


async def test_tags_set(cache: Cache):
    cache.register_tag("1", "key{i}")
    cache.register_tag("2", "key{i}")
    cache.register_tag("3", "key{i}")
    await cache.set("key1", "value", tags=["1", "2"])
    await cache.set("key2", "value", tags=["2"])
    await cache.set("key3", "value", tags=["3"])

    assert await cache.get_many("key1", "key2", "key3") == ("value", "value", "value")

    await cache.delete_tags("2")
    assert await cache.get_many("key1", "key2", "key3") == (None, None, "value")

    await cache.delete_tags("1")
    assert await cache.get_many("key1", "key2", "key3") == (None, None, "value")

    await cache.delete_tags("3")
    assert await cache.get_many("key1", "key2", "key3") == (None, None, None)


async def test_tags_incr(cache: Cache):
    cache.register_tag("tag", "key10")
    await cache.incr("key10", tags=["tag"])
    assert await cache.get("key10") == 1

    await cache.delete_tags("tag")
    assert await cache.get("key10") is None


async def test_set_delete_set_delete_tag(cache: Cache):
    cache.register_tag("tag", "key")
    await cache.set("key", "value", tags=["tag"])
    await cache.delete("key")  # should remove from tags
    await cache.set("key", "value2")  # without tag
    await cache.delete_tags("tag")

    assert await cache.get("key") == "value2"


async def test_set_expire_clean(cache: Cache):
    cache.register_tag("tag", "key{i}")
    assert await cache.set("key1", "test", expire=0.1, tags=["tag"])
    assert await cache.set("key2", "test", expire=0.1, tags=["tag"])
    assert await cache.set_pop("_tag:tag", count=1)
    await asyncio.sleep(0.11)
    assert not await cache.set_pop("_tag:tag")


@pytest.mark.xfail  # for now redis without client side and disk cache can not pass this test
async def test_set_expire_clean_2(cache: Cache):
    cache.register_tag("tag", "key{i}")
    assert await cache.set("key1", "test", expire=0.1, tags=["tag"])
    await asyncio.sleep(0.09)
    assert await cache.set("key2", "test", expire=0.1, tags=["tag"])
    await asyncio.sleep(0.02)
    # at this time first cache is expired and should not exist in set
    assert await cache.get("key1") is None
    assert await cache.get("key2") is not None
    assert list(await cache.set_pop("_tag:tag", count=1))[0] == "key2"
    await asyncio.sleep(0)
    assert not list(await cache.set_pop("_tag:tag"))


async def test_tags_gte_100(cache: Cache):
    cache.register_tag("all", "key_{i}")
    cache.register_tag("{i}", "key_{i}")

    for i in range(150):
        await cache.set(f"key_{i}", "value", tags=[f"{i}", "all"])

    await cache.delete_tags("1", "2", "100")
    await cache.delete_tags("1", "2", "100")

    await cache.delete_tags("5", "all")


async def test_tag_decorator(cache: Cache):
    @cache(ttl="1m", key="key:{a}:{b}", tags=["all", "tag:{a}"])
    async def func(a, b=None):
        return random()

    first = await func(1)
    second = await func(2)
    one_more = await func(5)

    await cache.delete_tags("tag:1")

    assert await func(1) != first
    assert await func(2) == second

    await cache.delete_tags("all")

    assert await func(2) != second
    assert await func(5) != one_more


async def test_tag_soft_decorator(cache: Cache):
    @cache.soft(ttl="2m", key="key:{a}", tags=["all", "tag:{a}"])
    async def func(a):
        return random()

    first = await func(1)
    second = await func(2)
    one_more = await func(5)

    await cache.delete_tags("tag:1")

    assert await func(1) != first
    assert await func(2) == second

    await cache.delete_tags("all")

    assert await func(2) != second
    assert await func(5) != one_more


async def test_tag_early_decorator(cache: Cache):
    @cache.early(ttl="2m", early_ttl="10s", key="key:{a}", tags=["all", "tag:{a}"])
    async def func(a):
        return random()

    first = await func(1)
    second = await func(2)
    one_more = await func(5)

    await cache.delete_tags("tag:1")

    assert await func(1) != first
    assert await func(2) == second

    await cache.delete_tags("all")

    assert await func(2) != second
    assert await func(5) != one_more


async def test_tag_hit_decorator(cache: Cache):
    @cache.dynamic(ttl="2m", key="key:{a}", tags=["all", "tag:{a}"])
    async def func(a):
        return random()

    first = await func(1)
    second = await func(2)
    one_more = await func(5)

    await cache.delete_tags("tag:1")

    assert await func(1) != first
    assert await func(2) == second

    await cache.delete_tags("all")

    assert await func(2) != second
    assert await func(5) != one_more


async def test_double_decorator(cache: Cache):
    @cache(ttl="1m", key="key:{a}", tags=["all"])
    @cache.soft(ttl="1m", key="key:{a}", tags=["all", "tag:{a}"])
    async def func(a):
        return random()

    second = await func(2)
    one_more = await func(5)

    await cache.delete_tags("all")

    assert await func(2) != second
    assert await func(5) != one_more


@pytest.mark.redis
async def test_delete_tags_separate_backend(cache: Cache, redis_dsn: str):
    tag_backend = cache.setup_tags_backend(redis_dsn)
    tag_backend.set_pop = AsyncMock(side_effect=[["key", "key2"], []])
    tag_backend.init = AsyncMock(wraps=tag_backend.init)

    cache.register_tag(tag="tag", key_template="key")

    await cache.delete_tags("tag")

    tag_backend.set_pop.assert_awaited_with(key="_tag:tag", count=100)
    tag_backend.init.assert_awaited_once()
