import asyncio
from datetime import timedelta

import pytest
import pytest_asyncio

from cashews import mem

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


# just an alias, required by aiohttp
@pytest_asyncio.fixture()
async def loop():
    return asyncio.get_running_loop()


@pytest.fixture(name="app")
def _app():
    from aiohttp import web

    def json(func):
        async def _handler(request):
            return web.json_response(data=await func(request))

        return _handler

    @json
    @mem(ttl=timedelta(seconds=1), key="q:{request.query}")
    async def handle(request):
        name = request.headers.get("X-Name", "test")
        return {"name": name, "q": request.query.get("q", "q")}

    app = web.Application()
    app.add_routes([web.get("/", handle)])
    return app


@pytest.fixture(name="cli")
async def _cli(aiohttp_client, app):
    return await aiohttp_client(app)


async def test_no_cache(cli):
    resp = await cli.get("/")

    assert resp.status == 200
    assert await resp.json() == {"name": "test", "q": "q"}

    resp = await cli.get("/?q=test", headers={"X-Name": "name"})
    assert resp.status == 200
    assert await resp.json() == {"name": "name", "q": "test"}


async def test_cache(cli):
    response = await cli.get("/")
    assert response.status == 200
    assert await response.json() == {"name": "test", "q": "q"}

    response = await cli.get("/", headers={"X-Name": "name"})
    assert response.status == 200
    assert await response.json() == {"name": "test", "q": "q"}
