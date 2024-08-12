import pytest

from cashews import Cache
from cashews.helpers import add_prefix


@pytest.fixture(autouse=True)
def _add_prefix(cache: Cache, target):
    cache._add_backend(target, (add_prefix("prefix!"),))


async def test_add_prefix_get(cache: Cache, target):
    await cache.get(key="key")
    target.get.assert_called_once_with(key="prefix!key", default=None)


async def test_add_prefix_set(cache: Cache, target):
    await cache.set(key="key", value="value")
    target.set.assert_called_once_with(
        key="prefix!key",
        value="value",
        exist=None,
        expire=None,
    )


async def test_add_prefix_ping(cache: Cache, target):
    await cache.ping()
    target.ping.assert_called_once_with(message=b"PING")


async def test_add_prefix_get_many(cache: Cache, target):
    await cache.get_many("key")
    target.get_many.assert_called_once_with("prefix!key")


async def test_add_prefix_set_many(cache: Cache, target):
    await cache.set_many({"key": "value"})
    target.set_many.assert_called_once_with(pairs={"prefix!key": "value"}, expire=None)


async def test_add_prefix_delete(cache: Cache, target):
    await cache.delete("key")
    target.delete.assert_called_once_with(key="prefix!key")


async def test_add_prefix_delete_match(cache: Cache, target):
    await cache.delete_match("key")
    target.delete_match.assert_called_once_with(pattern="prefix!key")


async def test_add_prefix_delete_many(cache: Cache, target):
    await cache.delete_many("key")
    target.delete_many.assert_called_once_with("prefix!key")
