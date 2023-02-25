import pytest

from cashews import Cache
from cashews.helpers import add_prefix, all_keys_lower, memory_limit

pytestmark = pytest.mark.asyncio


async def test_all_keys_lower(cache: Cache, target):
    cache._add_backend(target, (all_keys_lower(),))
    await cache.get(key="KEY")
    target.get.assert_called_once_with(key="key", default=None)

    await cache.set(key="KEY", value="value")
    target.set.assert_called_once_with(
        key="key",
        value="value",
        exist=None,
        expire=None,
    )
    await cache.set_many({"KEY": "value"})
    target.set_many.assert_called_once_with(
        pairs={"key": "value"},
        expire=None,
    )
    await cache.ping()
    target.ping.assert_called_once_with(message=b"PING")


async def test_memory_limit(cache: Cache, target):
    cache._add_backend(target, (memory_limit(min_bytes=52, max_bytes=75),))

    await cache.set(key="key", value="v")
    target.set.assert_not_called()

    await cache.set(key="key", value="v" * 31)
    target.set.assert_not_called()

    await cache.set(key="key", value="v" * 15)
    target.set.assert_called_once()

    await cache.ping()
    target.ping.assert_called_once_with(message=b"PING")


async def test_add_prefix(cache: Cache, target):
    cache._add_backend(target, (add_prefix("prefix!"),))

    await cache.get(key="key")
    target.get.assert_called_once_with(key="prefix!key", default=None)

    await cache.set(key="key", value="value")
    target.set.assert_called_once_with(
        key="prefix!key",
        value="value",
        exist=None,
        expire=None,
    )
    await cache.ping()
    target.ping.assert_called_once_with(message=b"PING")


async def test_add_prefix_get_many(cache: Cache, target):
    cache._add_backend(target, (add_prefix("prefix!"),))
    await cache.get_many("key")
    target.get_many.assert_called_once_with("prefix!key")


async def test_add_prefix_set_many(cache: Cache, target):
    cache._add_backend(target, (add_prefix("prefix!"),))
    await cache.set_many({"key": "value"})
    target.set_many.assert_called_once_with(pairs={"prefix!key": "value"}, expire=None)


async def test_add_prefix_delete_match(cache: Cache, target):
    cache._add_backend(target, (add_prefix("prefix!"),))
    await cache.delete_match("key")
    target.delete_match.assert_called_once_with(pattern="prefix!key")
