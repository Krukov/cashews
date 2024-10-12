import pytest

from cashews import Cache
from cashews.helpers import memory_limit


@pytest.mark.parametrize(
    ("min_bytes", "max_bytes", "size", "called"),
    (
        (10, 80, 11, True),
        (10, 80, 10, True),
        (10, 80, 80, False),
        (1, 11, 10, False),
        (10, 1, 80, False),
    ),
)
async def test_memory_limit_set(cache: Cache, target, min_bytes, max_bytes, size, called):
    cache._add_backend(target, (memory_limit(min_bytes=min_bytes, max_bytes=max_bytes),))

    await cache.set(key="key", value="v" * size)

    if called:
        target.set.assert_called_once_with(key="key", value="v" * size, expire=None, exist=None)
    else:
        target.set.assert_not_called()


async def test_memory_limit_set_many(cache: Cache, target):
    cache._add_backend(target, (memory_limit(min_bytes=52, max_bytes=75),))

    await cache.set_many({"key": "v" * 35})
    target.set_many.assert_not_called()

    await cache.set_many({"key": "v" * 35, "key2": "v"})
    target.set_many.assert_not_called()

    await cache.set_many({"key": "v" * 35, "key2": "v", "key3": "v" * 15})
    target.set_many.assert_called_once_with(pairs={"key3": "v" * 15}, expire=None)
