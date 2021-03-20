import asyncio
import sys
from unittest.mock import Mock

import pytest

from cashews.backends.interface import Backend, ProxyBackend
from cashews.backends.memory import Memory

pytestmark = pytest.mark.asyncio


@pytest.fixture(
    name="cache",
    params=[
        "memory",
        pytest.param("redis", marks=pytest.mark.redis),
        pytest.param("diskcache", marks=pytest.mark.diskcache),
    ],
)
async def _cache(request, redis_dsn, backend_factory):
    if request.param == "diskcache":
        from cashews.backends.diskcache import DiskCache

        backend = await backend_factory(DiskCache, shards=0)
        yield backend
    elif request.param == "redis":
        from cashews.backends.redis import Redis

        yield await backend_factory(Redis, redis_dsn, hash_key=None)
    else:
        yield await backend_factory(Memory)


async def test_set_get(cache):
    await cache.set("key", b"value")
    assert await cache.get("key") == b"value"


async def test_set_get_many(cache):
    await cache.set("key", b"value")
    assert await cache.get_many("key", "no_exists") == (b"value", None)


async def test_set_exist(cache):
    assert await cache.set("key", b"value")
    assert await cache.set("key", b"value", exist=True)
    assert not await cache.set("key2", b"value", exist=True)

    assert await cache.set("key2", b"value", exist=False)
    assert not await cache.set("key2", b"value", exist=False)


async def test_get_no_value(cache):
    assert await cache.get("key2") is None


async def test_incr(cache):
    assert await cache.incr("incr") == 1
    assert await cache.incr("incr") == 2
    assert await cache.get("incr") == 2


async def test_incr_setted(cache):
    await cache.set("incr", "test")
    with pytest.raises(Exception):
        assert await cache.incr("incr") == 1
    assert await cache.get("incr") == "test"


async def test_ping(cache):
    assert await cache.ping() == b"PONG"


async def test_exists(cache):
    assert not await cache.exists("not")
    await cache.set("yes", "1")
    assert await cache.exists("yes")


async def test_expire(cache):
    await cache.set("key", b"value", expire=0.01)
    assert await cache.get("key") == b"value"
    await asyncio.sleep(0.011)
    assert await cache.get("key") is None


async def test_get_set_expire(cache):
    await cache.set("key", b"value")
    assert await cache.get("key") == b"value"
    assert await cache.get_expire("key") == -1
    await cache.expire("key", 1)
    assert await cache.get_expire("key") == 1


@pytest.mark.parametrize(
    ("method", "args", "defaults"),
    (
        ("get", ("key",), {"default": None}),
        ("get_row", ("key",), None),
        ("set", ("key", "value"), {"exist": None, "expire": None}),
        ("set_row", ("key", "value"), None),
        ("incr", ("key",), None),
        ("exists", ("key",), None),
        ("delete", ("key",), None),
        ("expire", ("key", 10), None),
        ("ping", (), None),
        ("clear", (), None),
        ("set_lock", ("key", "value", 10), None),
        ("unlock", ("key", "value"), None),
        ("is_locked", ("key",), {"wait": None, "step": 0.1}),
    ),
)
async def test_proxy_backend(method, args, defaults):
    target = Mock(wraps=Backend())
    backend = ProxyBackend(target=target)

    await getattr(backend, method)(*args)
    if defaults:
        getattr(target, method).assert_called_once_with(*args, **defaults)
    else:
        getattr(target, method).assert_called_once_with(*args)


async def test_delete_match(cache: Backend):
    await cache.set("pref:test:test", b"value")
    await cache.set("pref:value:test", b"value2")
    await cache.set("pref:-:test", b"-")
    await cache.set("pref:*:test", b"*")

    await cache.set("ppref:test:test", b"value3")
    await cache.set("pref:test:tests", b"value3")

    await cache.delete_match("pref:*:test")

    assert await cache.get("pref:test:test") is None
    assert await cache.get("pref:value:test") is None
    assert await cache.get("pref:-:test") is None
    assert await cache.get("pref:*:test") is None

    assert await cache.get("ppref:test:test") is not None
    assert await cache.get("pref:test:tests") is not None


async def test_get_size(cache: Backend):
    await cache.set("test", b"1")
    assert await cache.get_size("test") in (
        sys.getsizeof((None, b"1")) + sys.getsizeof(b"1") + sys.getsizeof(None),
        68,
        -1,
    )  # 106 - ordered dict,  68 redis, -1 for diskcache


async def test_lru(backend_factory):
    cache = await backend_factory(Memory, size=10)
    # full cache
    for i in range(10):
        await cache.set(f"key:{i}", i)

    # use only 5 first
    for i in range(5):
        await cache.get(f"key:{i}")

    # add 5 more keys
    for i in range(5):
        await cache.set(f"key:{i}:new", i)

    assert len(cache.store) == 10

    for i in range(5):
        assert await cache.get(f"key:{i}") == i

    for i in range(6, 10):
        assert await cache.get(f"key:{i}") == None
