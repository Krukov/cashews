import asyncio
import random
import string
from datetime import timedelta

from fastapi import FastAPI

from cashews import Cache, LockedError, RateLimitError, cache, context_cache_detect, mem, utils  # noqa: F401

app = FastAPI()
cache.setup("mem://")
# cache.setup("disk://")
# cache.setup("redis://?safe=True&maxsize=20&create_connection_timeout=0.01")
# cache.setup("redis://?safe=True&maxsize=20&create_connection_timeout=0.01", client_side=True)
# cache.setup("redis://?safe=True&maxsize=20&create_connection_timeout=0.01", client_side=True, local_cache=Cache("disk").setup("disk://"))  # noqa: 139


@app.get("/")
@cache(ttl=timedelta(minutes=1))
async def simple():
    await asyncio.sleep(1)
    return "".join([random.choice(string.ascii_letters) for _ in range(10)])


@app.get("/early")
@cache.early(ttl=timedelta(minutes=5), early_ttl=timedelta(minutes=1))
async def early():
    await asyncio.sleep(1)
    return "".join([random.choice(string.ascii_letters) for _ in range(10)])


@app.get("/hit")
@cache.hit(ttl=timedelta(minutes=1), cache_hits=100, update_before=80)
async def hit():
    await asyncio.sleep(1)
    return "".join([random.choice(string.ascii_letters) for _ in range(10)])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
