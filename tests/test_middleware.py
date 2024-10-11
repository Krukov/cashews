from cashews import Cache
from cashews.helpers import memory_limit


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
