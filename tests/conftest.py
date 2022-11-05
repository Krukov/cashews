from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

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
def backend_factory():
    backend = None

    async def factory(backend_cls: type[Backend], *args, **kwargs):
        nonlocal backend

        backend = backend_cls(*args, **kwargs)
        await backend.init()
        await backend.clear()
        await asyncio.sleep(0.001)

        return backend

    try:
        yield factory
    finally:
        if backend is not None:
            backend.close()
