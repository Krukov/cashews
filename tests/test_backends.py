import asyncio
from unittest.mock import Mock

import pytest
from cashews.backends.interface import Backend, ProxyBackend
from cashews.backends.memory import Memory

pytestmark = pytest.mark.asyncio


@pytest.fixture(name="cache")
async def _cache():
    return Memory()


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


async def test_ping(cache):
    assert await cache.ping() == b"PONG"


async def test_expire(cache):
    await cache.set("key", b"value", expire=0.01)
    assert await cache.get("key") == b"value"
    await asyncio.sleep(0.01)
    assert await cache.get("key") is None


@pytest.mark.parametrize(
    ("method", "args", "defaults"),
    (
        ("get", ("key",), None),
        ("set", ("key", "value"), {"exist": None, "expire": None}),
        ("incr", ("key",), None),
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
