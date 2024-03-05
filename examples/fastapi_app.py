import asyncio
import os
import random
import string
import time

from fastapi import FastAPI, Header, Query
from fastapi.responses import StreamingResponse
from prometheus_client import make_asgi_app

from cashews import cache
from cashews.contrib.fastapi import (
    CacheDeleteMiddleware,
    CacheEtagMiddleware,
    CacheRequestControlMiddleware,
    cache_control_ttl,
)
from cashews.contrib.prometheus import create_metrics_middleware

app = FastAPI()
app.add_middleware(CacheDeleteMiddleware)
app.add_middleware(CacheEtagMiddleware)
app.add_middleware(CacheRequestControlMiddleware)

metrics_middleware = create_metrics_middleware(with_tag=True)
cache.setup(os.environ.get("CACHE_URI", "redis://"), middlewares=(metrics_middleware,))
cache.setup("mem://", middlewares=(metrics_middleware,), prefix="srl")
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)
KB = 1024


@app.get("/")
@cache.failover(ttl="1h")
@cache.slice_rate_limit(limit=10, period="3m", key="rate:{user_agent:hash}")
@cache(
    ttl=cache_control_ttl(default="4m"),
    key="simple:{user_agent:hash}",
    time_condition="1s",
    tags=("simple",),
)
async def simple(user_agent: str = Header("No")):
    await asyncio.sleep(1.1)
    return "".join([random.choice(string.ascii_letters) for _ in range(10)])


@app.get("/stream")
@cache(ttl="1m", key="stream:{file_path}")
async def stream(file_path: str = Query(__file__)):
    return StreamingResponse(_read_file(file_path=file_path))


async def _read_file(*, file_path, chunk_size=10 * KB):
    loop = asyncio.get_running_loop()
    with open(file_path, encoding="latin1") as file_obj:
        while True:
            chunk = await loop.run_in_executor(None, file_obj.read, chunk_size)
            if not chunk:
                return
            yield chunk


@app.get("/early")
@cache.early(ttl="5m", early_ttl="1m", key="simple:{user_agent:hash}")
async def early(user_agent: str = Header("No")):
    return "".join([random.choice(string.ascii_letters) for _ in range(10)])


@app.middleware("http")
async def add_process_time_header(request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = time.perf_counter() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
