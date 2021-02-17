from datetime import timedelta

import pytest

from cashews import mem

pytestmark = [pytest.mark.integration]


@pytest.fixture(name="app")
def _app():
    from fastapi import FastAPI, Header

    app = FastAPI()

    @app.get("/")
    @mem.early(ttl=timedelta(seconds=1), key="root:{q}")
    async def root(q: str = "q", x_name: str = Header("test")):
        return {"name": x_name, "q": q}

    return app


@pytest.fixture(name="client")
def _client(app):
    from starlette.testclient import TestClient

    return TestClient(app)


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
