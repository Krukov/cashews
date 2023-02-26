import random

import pytest

from cashews import Cache

pytestmark = [pytest.mark.integration]


@pytest.fixture(
    name="app",
    scope="session",
    params=[
        "memory",
        pytest.param("redis", marks=pytest.mark.integration),
        pytest.param("redis_cs", marks=pytest.mark.integration),
        pytest.param("diskcache", marks=pytest.mark.integration),
    ],
)
def _app(request, redis_dsn):
    dsn = "mem://"
    if request.param == "diskcache":
        dsn = "disk://"
    elif request.param == "redis":
        dsn = redis_dsn
    elif request.param == "redis_cs":
        dsn = redis_dsn + "&client_side=t"
    cache = Cache()
    cache.setup(dsn)

    from fastapi import FastAPI, Header
    from starlette.responses import StreamingResponse

    app = FastAPI()

    @app.get("/")
    @cache.early(ttl="1s", key="root:{q}")
    async def root(q: str = "q", x_name: str = Header("test")):
        return {"name": x_name, "q": q}

    @cache.iterator(ttl="5s")
    async def iterator():
        for i in range(10):
            yield f"{random.randint(0, 9)}"

    @app.get("/stream")
    async def stream():
        return StreamingResponse(iterator(), status_code=201, headers={"X-Test": "TRUE"})

    return app, cache


@pytest.fixture(name="client", scope="session")
def _client(app):
    from starlette.testclient import TestClient

    with TestClient(app[0]) as client:
        yield client


def test_no_cache(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"name": "test", "q": "q"}

    response = client.get("/?q=test", headers={"X-Name": "name"})
    assert response.status_code == 200
    assert response.json() == {"name": "name", "q": "test"}


def test_cache(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"name": "test", "q": "q"}

    response = client.get("/", headers={"X-Name": "name"})
    assert response.status_code == 200
    assert response.json() == {"name": "test", "q": "q"}


def test_cache_stream(client, app):
    response = client.get("/stream")
    assert response.status_code == 201
    assert response.headers["X-Test"] == "TRUE"
    was = response.content

    response = client.get("/stream")
    assert response.status_code == 201
    assert response.headers["X-Test"] == "TRUE"
    assert response.content == was
