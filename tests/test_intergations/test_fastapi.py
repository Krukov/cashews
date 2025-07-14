from contextlib import asynccontextmanager, contextmanager
from random import random
from unittest.mock import Mock

import pytest

from cashews import Cache

pytestmark = [pytest.mark.integration]


@pytest.fixture(
    name="cache",
    params=[
        "memory",
        pytest.param("redis", marks=pytest.mark.integration),
        pytest.param("redis_cs", marks=pytest.mark.integration),
        pytest.param("diskcache", marks=pytest.mark.integration),
    ],
)
async def _cache(request, redis_dsn):
    dsn = "mem://"
    if request.param == "diskcache":
        dsn = "disk://"
    elif request.param == "redis":
        dsn = redis_dsn
    elif request.param == "redis_cs":
        dsn = redis_dsn + "&client_side=t"
    cache = Cache()
    cache.setup(dsn, suppress=False)
    yield cache
    await cache.clear()


@pytest.fixture(name="app")
def _app(cache):
    from fastapi import FastAPI, Header

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await cache.close()

    app = FastAPI(lifespan=lifespan)

    @app.get("/")
    @cache.early(ttl="1s", key="root:{q}")
    async def root(q: str = "q", x_name: str = Header("test")):
        return {"name": x_name, "q": q}

    return app


@pytest.fixture(name="client")
def _client(app):
    from starlette.testclient import TestClient

    with TestClient(app) as client:
        yield client


@pytest.fixture(name="client_with_middleware")
def _client_with_middleware(app):
    from starlette.testclient import TestClient

    @contextmanager
    def _client(middleware, **kwargs):
        app.add_middleware(middleware, **kwargs)

        with TestClient(app) as client:
            yield client

    return _client


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


def test_cache_stream(client, app, cache):
    from starlette.responses import StreamingResponse

    call = Mock()

    def iterator():
        call()
        for i in range(10):
            yield f"{i}"

    @app.get("/stream")
    @cache(ttl="10s", key="stream")
    async def stream():
        return StreamingResponse(iterator(), status_code=201, headers={"X-Test": "TRUE", "X-Header": "Some;:=Value"})

    response = client.get("/stream")
    assert response.status_code == 201
    assert response.headers["X-Test"] == "TRUE"
    assert response.headers["X-Header"] == "Some;:=Value"
    assert response.content == b"0123456789"

    response = client.get("/stream")
    call.assert_called_once()
    assert response.status_code == 201
    assert response.headers["X-Test"] == "TRUE"
    assert response.content == b"0123456789"


def test_cache_stream_on_error(client, app, cache):
    from starlette.responses import StreamingResponse

    def iterator():
        for i in range(10):
            if i == 5:
                raise Exception
            yield f"{i}"

    @app.get("/stream")
    @cache(ttl="10s", key="stream")
    async def stream():
        return StreamingResponse(iterator(), status_code=201, headers={"X-Test": "TRUE"})

    with pytest.raises(Exception):
        client.get("/stream")
    with pytest.raises(Exception):
        client.get("/stream")


def test_cache_delete_middleware(client_with_middleware, app, cache):
    from cashews.contrib.fastapi import CacheDeleteMiddleware

    @app.get("/to_delete")
    @cache(ttl="10s", key="to_delete")
    async def rand():
        return random()

    with client_with_middleware(CacheDeleteMiddleware) as client:
        response = client.get("/to_delete")
        response2 = client.get("/to_delete")
        assert response.content == response2.content

        response3 = client.get("/to_delete", headers={"Clear-Site-Data": "cache"})
        assert response.content != response3.content


def test_cache_etag(client_with_middleware, app, cache):
    from cashews.contrib.fastapi import CacheEtagMiddleware

    @app.get("/to_cache")
    @cache(ttl="10s", key="to_cache")
    async def rand():
        return str(random()).encode()

    with client_with_middleware(CacheEtagMiddleware, cache_instance=cache) as client:
        response = client.get("/to_cache")
        etag = response.headers["ETag"]

        response2 = client.get("/to_cache", headers={"If-None-Match": etag})
        assert response2.status_code == 304

        response3 = client.get("/to_cache", headers={"If-None-Match": str(random())})
        assert response3.status_code == 200
        assert response.content == response3.content
        assert etag == response3.headers["ETag"]


def test_cache_etag_early(client_with_middleware, app, cache):
    from cashews.contrib.fastapi import CacheEtagMiddleware

    @app.get("/to_cache")
    @cache.early(ttl="10s", early_ttl="7s", key="to_cache")
    async def rand():
        return str(random()).encode()

    with client_with_middleware(CacheEtagMiddleware, cache_instance=cache) as client:
        response = client.get("/to_cache")
        etag = response.headers["ETag"]

        response2 = client.get("/to_cache", headers={"If-None-Match": etag})
        assert response2.status_code == 304

        response3 = client.get("/to_cache", headers={"If-None-Match": str(random())})
        assert response3.status_code == 200
        assert response.content == response3.content
        assert etag == response3.headers["ETag"]


@pytest.fixture(name="app_with_cache_control")
def _app_with_cache_control(cache, app):
    from cashews.contrib.fastapi import CacheRequestControlMiddleware, cache_control_ttl

    app.add_middleware(CacheRequestControlMiddleware, cache_instance=cache)

    @app.get("/to_cache")
    @cache(ttl=cache_control_ttl("10s"), key="to_cache")
    async def rand():
        return random()


@pytest.mark.usefixtures("app_with_cache_control")
def test_cache_control_max_age_0(client, cache):
    response1 = client.get("/to_cache", headers={"Cache-Control": "public, max-age=0"})
    response2 = client.get("/to_cache")
    assert response1.content != response2.content


@pytest.mark.usefixtures("app_with_cache_control")
def test_cache_control_max_age(client, cache):
    response1 = client.get("/to_cache", headers={"Cache-Control": "public, max-age=600"})
    response2 = client.get("/to_cache")
    assert response1.content == response2.content


@pytest.mark.usefixtures("app_with_cache_control")
def test_cache_control_no_headers(client, cache):
    response1 = client.get("/to_cache")
    response2 = client.get("/to_cache")
    assert response1.content == response2.content


@pytest.mark.usefixtures("app_with_cache_control")
def test_cache_control_no_cache(client, cache):
    response1 = client.get("/to_cache", headers={"Cache-Control": "no-cache"})
    response2 = client.get("/to_cache")
    response3 = client.get("/to_cache", headers={"Cache-Control": "no-cache"})

    assert response1.content == response2.content
    assert response1.content != response3.content
    assert response2.content != response3.content


@pytest.mark.usefixtures("app_with_cache_control")
def test_cache_control_no_store(client, cache):
    response1 = client.get("/to_cache", headers={"Cache-Control": "no-store"})
    response2 = client.get("/to_cache")
    assert response1.content != response2.content
