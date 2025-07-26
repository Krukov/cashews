import time

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from uvicorn import run

from cashews import cache
from cashews.contrib.fastapi import CacheEtagMiddleware

cache.setup("mem://")


async def cache_endpoint(request: Request) -> JSONResponse:
    cache_dict = {}
    async for k, v in cache.get_match("fastapi:*"):
        cache_dict[k] = v
    return JSONResponse(
        {
            "cache": cache_dict,
            "headers": dict(request.headers),
        }
    )


@cache(ttl="1h", key="time:")
async def time_endpoint(request: Request) -> JSONResponse:
    return JSONResponse({"time": time.monotonic_ns()})


routes = [Route("/cache", cache_endpoint, methods=["GET"]), Route("/time", time_endpoint, methods=["GET"])]

app = Starlette(routes=routes)
app.add_middleware(CacheEtagMiddleware)

if __name__ == "__main__":
    run(app, host="127.0.0.1", port=8000)
