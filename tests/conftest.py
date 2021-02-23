from __future__ import annotations

import os
from typing import TYPE_CHECKING, Type

import pytest

if TYPE_CHECKING:
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
    port = os.getenv("REDIS_PORT", "")
    return f"redis://{host}:{port}/"


@pytest.fixture
async def backend_factory():
    backend = None

    async def factory(backend_cls: Type[Backend], *args, **kwargs):
        nonlocal backend

        backend = backend_cls(*args, **kwargs)
        await backend.init()
        await backend.clear()

        return backend

    try:
        yield factory
    finally:
        assert backend is not None, "Fixture `backend_factory` wasn't called."
        backend.close()
