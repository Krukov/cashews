from datetime import timedelta

import pytest
from aiohttp import web
from cashews import mem

pytestmark = pytest.mark.asyncio
pytest_plugins = ["aiohttp.pytest_plugin"]


def json(func):
    async def _handler(request):
        return web.json_response(data=await func(request))

    return _handler


@json
@mem(ttl=timedelta(seconds=1), key="q:{request.query}")
async def handle(request: web.Request):
    name = request.headers.get("X-Name", "test")
    return {"name": name, "q": request.query.get("q", "q")}


@pytest.fixture(name="app")
def _app():
    app = web.Application()
    app.add_routes([web.get("/", handle)])
    return app


@pytest.fixture
def loop(event_loop):
    return event_loop


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
