from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING
from unittest.mock import Mock
from uuid import uuid4

import pytest
import pytest_asyncio

from cashews import Cache
from cashews.backends.memory import Memory
from cashews.backends.transaction import TransactionBackend

if TYPE_CHECKING:  # pragma: no cover
    from cashews.backends.interface import Backend


pytest_plugins = ["pytest_asyncio"]  # pylint: disable=invalid-name

try:
    import aiohttp
except ImportError:
    pass
else:
    pytest_plugins.append("aiohttp.pytest_plugin")

    del aiohttp


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def redis_dsn():
    host = os.getenv("REDIS_HOST", "")
    port = os.getenv("REDIS_PORT", "6379")
    return f"redis://{host}:{port}/"


@pytest.fixture(scope="session")
def backend_factory():
    def factory(backend_cls: type[Backend], *args, **kwargs):
        backend = backend_cls(*args, **kwargs)
        return backend

    return factory


@pytest_asyncio.fixture(
    name="raw_backend",
    scope="session",
    params=[
        "memory",
        "transactional",
        pytest.param("redis", marks=pytest.mark.redis),
        pytest.param("redis_hash", marks=pytest.mark.redis),
        pytest.param("redis_cs", marks=pytest.mark.redis),
        pytest.param("diskcache", marks=pytest.mark.diskcache),
    ],
)
async def _backend(request, redis_dsn, backend_factory):
    if request.param == "diskcache":
        from cashews.backends.diskcache import DiskCache

        backend = backend_factory(DiskCache, shards=0)
    elif request.param == "redis":
        from cashews.backends.redis import Redis

        backend = backend_factory(Redis, redis_dsn, hash_key=None, max_connections=20, safe=False, socket_timeout=10)
    elif request.param == "redis_hash":
        from cashews.backends.redis import Redis

        backend = backend_factory(
            Redis, redis_dsn, hash_key=uuid4().hex, max_connections=20, safe=False, socket_timeout=10
        )
    elif request.param == "redis_cs":
        from cashews.backends.redis.client_side import BcastClientSide

        backend = backend_factory(
            BcastClientSide, redis_dsn, hash_key=None, max_connections=5, safe=False, socket_timeout=10
        )
        backend._expire_for_recently_update = 0.1
    elif request.param == "transactional":
        backend = TransactionBackend(backend_factory(Memory))
    else:
        backend = backend_factory(Memory, check_interval=0.01)
    try:
        await backend.init()
        yield backend
    finally:
        await backend.close()


@pytest_asyncio.fixture()
async def backend(raw_backend):
    await raw_backend.clear()
    yield raw_backend


@pytest.fixture(scope="session")
def target(raw_backend):
    return Mock(wraps=raw_backend, is_full_disable=False, name=str(raw_backend))


@pytest_asyncio.fixture()
async def cache(target: Mock):
    await target.clear()
    target.reset_mock()
    cache = Cache()
    cache._add_backend(target)
    return cache
