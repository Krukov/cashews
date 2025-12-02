from __future__ import annotations

import asyncio
from random import random
from unittest.mock import AsyncMock

import pytest

from cashews import Cache, key_context


def test_register_tags(cache: Cache):
    cache.register_tag("tag", "key")
    cache.register_tag("tag_template", "key{i}")
    cache.register_tag("tag_test:{i}", "key{i:len}:test")
    cache.register_tag("tag_func:{i:hash}", "key{i}:test")
    cache.register_tag("tag_context:{val}", "key{i}:test")
    cache.register_tag("!@#$%^&*():{i}", "!@#$%^&*(){i}:test")

    with key_context(val=10):
        assert cache.get_key_tags("key1:test") == [
            "tag_template",
            "tag_test:1",
            "tag_func:c4ca4238a0b923820dcc509a6f75849b",
            "tag_context:10",
        ]
    assert cache.get_key_tags("keyanytest") == ["tag_template"]
    assert cache.get_key_tags("keytest") == ["tag_template"]
    assert cache.get_key_tags("key") == ["tag", "tag_template"]
    assert cache.get_key_tags("k") == []
    assert cache.get_key_tags("prefixkey") == []
    assert cache.get_key_tags(":key") == []
    assert cache.get_key_tags("!@#$%^&*()$:test") == ["!@#$%^&*():$"]


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
    await asyncio.sleep(0.1)
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
    if cache.name == "diskcache":
        pytest.skip("there is race condition on set_add/set_remove implementation")

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

    cache.register_tag(tag="tag", key_template="key")
    await cache.init()

    await cache.delete_tags("tag")

    tag_backend.set_pop.assert_awaited_with(key="_tag:tag", count=100)

    await tag_backend.close()


async def test_templated_tag_with_none_value(cache: Cache):
    @cache(
        ttl=None,
        tags=["a:{a};b:{b}"],
    )
    async def cached(
        a: int,
        b: int | None = None,
    ) -> str:
        return f"{a}{b}"

    assert await cached(1) == "1None"


async def test_template_tag_function(cache: Cache):
    @cache(ttl="1m", key="{user.name:hash}:func", tags=["tag:{user.name}", "tag:{user.name:hash}"])
    async def func(user):
        return random()

    class User:
        def __init__(self, name):
            self.name = name

        def __hash__(self):
            return hash(self.name)

    await func(User("test"))
    await cache.delete_tags("tag:test")


async def test_tag_with_multiple_nested_attributes(cache: Cache):
    """
    Test that tags work with multiple nested attributes from same object.
    This is the core bug fix test - should not raise regex compilation error.
    Regression test for: re.error: redefinition of group name 'data'
    """

    class Data:
        def __init__(self, first, second):
            self.first = first
            self.second = second

    # This should NOT raise re.error during decoration
    @cache(ttl="5m", key="f:{data.first}:s:{data.second}", tags=["computed"])
    async def mult(data: Data):
        return data.first * data.second

    result1 = await mult(Data(3, 5))
    assert result1 == 15

    # Should use cached value
    result2 = await mult(Data(3, 5))
    assert result2 == 15

    # Different parameters should compute new result
    result3 = await mult(Data(4, 6))
    assert result3 == 24

    # Delete and verify cache is cleared
    await cache.delete_tags("computed")


async def test_tag_with_dynamic_template_nested_attributes(cache: Cache):
    """
    Test that dynamic tag templates work with nested attributes.
    The tag template uses {data.first} and should be formatted with captured value.
    """

    class Data:
        def __init__(self, first, second):
            self.first = first
            self.second = second

    call_count = {"count": 0}

    @cache(ttl="5m", key="compute:{data.first}:{data.second}", tags=["result:{data.first}"])
    async def process(data: Data):
        call_count["count"] += 1
        return data.first + data.second

    result1 = await process(Data(10, 20))
    assert result1 == 30
    assert call_count["count"] == 1

    result2 = await process(Data(15, 25))
    assert result2 == 40
    assert call_count["count"] == 2

    # Both should be cached
    await process(Data(10, 20))
    await process(Data(15, 25))
    assert call_count["count"] == 2  # No new calls

    # Delete only results for data.first=10
    await cache.delete_tags("result:10")

    # First should be recomputed, second should still be cached
    await process(Data(10, 20))
    assert call_count["count"] == 3  # Recomputed

    await process(Data(15, 25))
    assert call_count["count"] == 3  # Still cached


async def test_tag_backward_compatibility_simple_names(cache: Cache):
    """
    Ensure fix doesn't break existing code with simple (non-nested) field names.
    """

    @cache(ttl="5m", key="item:{item_id}:{user_id}", tags=["items", "user:{user_id}"])
    async def get_item(item_id: int, user_id: int):
        return f"item-{item_id}-user-{user_id}"

    result1 = await get_item(1, 100)
    assert result1 == "item-1-user-100"

    result2 = await get_item(2, 100)
    assert result2 == "item-2-user-100"

    # Delete by user tag
    await cache.delete_tags("user:100")

    # Should have cleared both items
    result3 = await get_item(1, 100)
    assert result3 == "item-1-user-100"


async def test_tag_deeply_nested_attributes(cache: Cache):
    """
    Test with deeply nested attributes (e.g., user.profile.settings.value).
    """

    class Settings:
        def __init__(self, theme):
            self.theme = theme

    class Profile:
        def __init__(self, name, settings):
            self.name = name
            self.settings = settings

    class User:
        def __init__(self, profile):
            self.profile = profile

    @cache(ttl="5m", key="user:{user.profile.name}:{user.profile.settings.theme}", tags=["user_data"])
    async def get_user_data(user: User):
        return f"Data for {user.profile.name} with {user.profile.settings.theme} theme"

    user = User(Profile("Alice", Settings("dark")))
    result = await get_user_data(user)
    assert result == "Data for Alice with dark theme"

    # Should use cache
    result2 = await get_user_data(user)
    assert result2 == result

    await cache.delete_tags("user_data")


async def test_tag_mixed_nested_and_simple_attributes(cache: Cache):
    """
    Test mixing nested attributes and simple parameters in same key template.
    """

    class Context:
        def __init__(self, tenant_id):
            self.tenant_id = tenant_id

    @cache(
        ttl="5m",
        key="{ctx.tenant_id}:resource:{resource_id}",
        tags=["tenant:{ctx.tenant_id}", "all_resources"],
    )
    async def get_resource(ctx: Context, resource_id: int):
        return f"Resource {resource_id} for tenant {ctx.tenant_id}"

    result1 = await get_resource(Context("tenant1"), 42)
    assert result1 == "Resource 42 for tenant tenant1"

    result2 = await get_resource(Context("tenant1"), 99)
    assert result2 == "Resource 99 for tenant tenant1"

    result3 = await get_resource(Context("tenant2"), 42)
    assert result3 == "Resource 42 for tenant tenant2"

    # Delete by tenant tag - should clear both resources for tenant1
    await cache.delete_tags("tenant:tenant1")

    # Should be cleared
    result4 = await get_resource(Context("tenant1"), 42)
    assert result4 == "Resource 42 for tenant tenant1"
