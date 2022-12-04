import asyncio
import os
import random
import string
import time

from fastapi import FastAPI, Header

from cashews import Command, cache

app = FastAPI()
cache.setup(os.environ.get("CACHE_URI", "redis://?client_side=True"))


@app.get("/")
@cache.failover(ttl="1h")
@cache.rate_limit(10, "1m")
@cache(ttl="10m", key="simple:{user_agent}", time_condition="1s")
async def simple(user_agent: str = Header("No")):
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
