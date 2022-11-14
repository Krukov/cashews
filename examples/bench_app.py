import asyncio
import random
import string
import time
from datetime import timedelta

from fastapi import FastAPI

from cashews import Cache, Command, LockedError, RateLimitError, cache, context_cache_detect, mem, utils  # noqa: F401

app = FastAPI()
cache.setup("redis://", client_side=True)


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
@cache.hit(ttl=timedelta(minutes=1), cache_hits=100, update_after=80)
async def hit():
    await asyncio.sleep(1)
    return "".join([random.choice(string.ascii_letters) for _ in range(10)])


@app.middleware("http")
async def add_process_time_header(request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = time.perf_counter() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


@app.middleware("http")
async def add_from_cache_headers(request, call_next):
    with cache.detect as detector:
        response = await call_next(request)
        if request.method.lower() != "get":
            return response
        if detector.calls:
            response.headers["X-From-Cache-keys"] = ";".join(detector.calls.keys())
    return response


@app.middleware("http")
async def disable_middleware(request, call_next):
    if request.headers.get("X-No-Cache"):
        with cache.disabling(Command.GET):
            return await call_next(request)
    return await call_next(request)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
