from __future__ import annotations

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


@pytest.fixture
def redis_dsn():
    host = os.getenv("REDIS_HOST", "")
    port = os.getenv("REDIS_PORT", "6379")
    return f"redis://{host}:{port}/"


@pytest.fixture
def backend_factory():
    async def factory(backend_cls: type[Backend], *args, **kwargs):
        backend = backend_cls(*args, **kwargs)
        await backend.init()
        await backend.clear()
        return backend

    return factory


@pytest_asyncio.fixture(
    params=[
        "memory",
        "transactional",
        pytest.param("redis", marks=pytest.mark.redis),
        pytest.param("redis_hash", marks=pytest.mark.redis),
        pytest.param("redis_cs", marks=pytest.mark.redis),
        pytest.param("diskcache", marks=pytest.mark.diskcache),
    ],
)
async def backend(request, redis_dsn, backend_factory):
    if request.param == "diskcache":
        from cashews.backends.diskcache import DiskCache

        backend = await backend_factory(DiskCache, shards=0)
    elif request.param == "redis":
        from cashews.backends.redis import Redis

        backend = await backend_factory(
            Redis, redis_dsn, hash_key=None, max_connections=20, safe=False, socket_timeout=10
        )
    elif request.param == "redis_hash":
        from cashews.backends.redis import Redis

        backend = await backend_factory(
            Redis, redis_dsn, hash_key=uuid4().hex, max_connections=20, safe=False, socket_timeout=10
        )
    elif request.param == "redis_cs":
        from cashews.backends.redis.client_side import BcastClientSide

        backend = await backend_factory(
            BcastClientSide, redis_dsn, hash_key=None, max_connections=5, safe=False, socket_timeout=10
        )
    elif request.param == "transactional":
        backend = TransactionBackend(await backend_factory(Memory))
    else:
        backend = await backend_factory(Memory)
    try:
        yield backend
    finally:
        if request.param == "transactional":
            await backend.commit()
        await backend.close()


@pytest.fixture()
def target(backend):
    return Mock(wraps=backend, is_full_disable=False)


@pytest.fixture()
def cache(target):
    cache = Cache()
    cache._add_backend(target)
    return cache
