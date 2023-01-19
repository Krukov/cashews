from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from cashews.backends.memory import Memory

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
    name="backend",
    params=[
        "memory",
        pytest.param("redis", marks=pytest.mark.redis),
        pytest.param("diskcache", marks=pytest.mark.diskcache),
    ],
)
async def _backend(request, redis_dsn, backend_factory):
    if request.param == "diskcache":
        from cashews.backends.diskcache import DiskCache

        backend = await backend_factory(DiskCache)
        yield backend
    elif request.param == "redis":
        from cashews.backends.redis import Redis

        backend = await backend_factory(Redis, redis_dsn, max_connections=20, safe=False, socket_timeout=10)
        yield backend
    else:
        backend = await backend_factory(backend_cls=Memory)
        yield backend
    await backend.close()
