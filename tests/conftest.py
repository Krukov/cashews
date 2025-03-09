from __future__ import annotations

import os
import random
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from cashews import Cache
from cashews.backends.memory import Memory
from cashews.backends.transaction import TransactionBackend

if TYPE_CHECKING:  # pragma: no cover
    from cashews.backends.interface import Backend


pytest_plugins = []  # pylint: disable=invalid-name

try:
    import aiohttp
except ImportError:
    pass
else:
    pytest_plugins.append("aiohttp.pytest_plugin")

    del aiohttp


@pytest.fixture(scope="session")
def redis_dsn():
    host = os.getenv("REDIS_HOST", "")
    port = os.getenv("REDIS_PORT", "6379")
    db = random.choice(range(15))
    return f"redis://{host}:{port}/{db}"


@pytest.fixture(scope="session")
def backend_factory():
    def factory(backend_cls: type[Backend], *args, **kwargs):
        backend = backend_cls(*args, **kwargs)
        return backend

    return factory


@pytest.fixture(
    name="raw_backend",
    # scope="session",
    params=[
        "memory",
        "transactional",
        pytest.param("redis", marks=pytest.mark.redis),
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

        backend = backend_factory(
            Redis,
            redis_dsn,
            max_connections=20,
            suppress=False,
            socket_timeout=1,
            wait_for_connection_timeout=1,
        )
    elif request.param == "redis_cs":
        from cashews.backends.redis.client_side import BcastClientSide

        backend = backend_factory(
            BcastClientSide,
            redis_dsn,
            max_connections=5,
            suppress=False,
            socket_timeout=0.1,
        )
        backend._expire_for_recently_update = 0.1
    elif request.param == "transactional":
        backend = TransactionBackend(backend_factory(Memory))
    else:
        backend = backend_factory(Memory, check_interval=0.01)
    try:
        await backend.init()
        await backend.clear()
        yield backend, request.param
    finally:
        await backend.close()


@pytest.fixture(name="target")
def _target(raw_backend):
    backend, _ = raw_backend
    return Mock(wraps=backend, is_full_disable=False)


@pytest.fixture()
async def cache(target: Mock, raw_backend):
    await target.clear()
    target.reset_mock()
    cache = Cache(name=raw_backend[1])
    cache._add_backend(target)
    return cache
