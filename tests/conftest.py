import os

import pytest

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
