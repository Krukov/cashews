from datetime import timedelta

from cashews import mem
from fastapi import FastAPI, Header
from starlette.testclient import TestClient

app = FastAPI()


@app.get("/")
@mem.early(ttl=timedelta(seconds=1), key="root:{q}")
async def root(q: str = "q", x_name: str = Header("test")):
    return {"name": x_name, "q": q}


client = TestClient(app)


def test_no_cache():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"name": "test", "q": "q"}

    response = client.get("/?q=test", headers={"X-Name": "name"})
    assert response.status_code == 200
    assert response.json() == {"name": "name", "q": "test"}


def test_cache():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"name": "test", "q": "q"}

    response = client.get("/", headers={"X-Name": "name"})
    assert response.status_code == 200
    assert response.json() == {"name": "test", "q": "q"}
