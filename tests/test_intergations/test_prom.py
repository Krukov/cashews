import pytest

from cashews import Cache

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.fixture(scope="session", name="middleware")
def _middleware():
    from cashews.contrib.prometheus import create_metrics_middleware

    return create_metrics_middleware()


async def test_smoke(middleware):
    cache = Cache()
    cache.setup("mem://", middlewares=(middleware,))
    await cache.get("smoke")
