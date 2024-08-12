from cashews import Cache
from cashews.helpers import all_keys_lower, memory_limit


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

    await cache.set(key="key", value="v" * 35)
    target.set.assert_not_called()

    await cache.set(key="key", value="v" * 15)
    target.set.assert_called_once()

    await cache.ping()
    target.ping.assert_called_once_with(message=b"PING")
