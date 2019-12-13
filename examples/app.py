import asyncio
import datetime
import logging
import random
import time
from decimal import Decimal
import dataclasses

from cashews import cache, early, fail, locked
from fastapi import FastAPI

logging.basicConfig(level=logging.DEBUG)

# cache2 = Cache()
# cache2.setup("mem://")
cache.setup(
    "redis://0.0.0.0/",
    db=2,
    safe=False,
    create_connection_timeout=0.5,
    hash_key=b"test",
    # compress_level=9,
)
# cache.setup("mem://")

app = FastAPI()


@app.get("/cache/")
@cache(ttl=10)
async def get():
    await asyncio.sleep(1)
    return {"status": "ok"}


@app.get("/cache/early/")
@early(ttl=10)
async def get2():
    print("HIT!", datetime.datetime.utcnow().isoformat())
    await asyncio.sleep(1)
    return {"status": "ok"}


@app.get("/cache/fail/")
@fail(ttl=100)
async def get3():
    if random.randint(1, 10) == 1:
        raise Exception("error")
    return {"status": "ok"}


@app.get("/cache/rate/")
@cache.rate_limit(1, period=10, ttl=60)
async def get4():
    return {"status": "ok"}


@app.get("/cache/lock")
@locked(ttl=10)
async def get7():
    print("HIT!", datetime.datetime.utcnow().isoformat())
    await asyncio.sleep(1)
    return {"status": "ok"}


@app.get("/cache/elock")
@early(ttl=20)
@locked(ttl=11, lock_ttl=10)
async def get9():
    print("HIT!", datetime.datetime.utcnow().isoformat())
    await asyncio.sleep(10)
    return {"status": "ok"}


@app.get("/big/")
@cache(ttl=100)
async def big():
    return {"status": [{"number": i} for i in range(1000)]}


@dataclasses.dataclass()
class TestDC:
    test: str


@app.get("/")
async def all_():
    start = time.time()
    await cache.set(
        "key",
        [
            {"field": random.randint(0, 100), "name": random.randint(0, 100), "value": Decimal("10.001")}
            for _ in range(100)
        ],
    )
    await cache.get("key")
    return {"status": time.time() - start}
