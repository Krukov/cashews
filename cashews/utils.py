import time
import uuid
from statistics import mean

from .backends.interface import Backend
from .key import template_to_pattern


async def _get_latency(func, **kwargs) -> float:
    start = time.perf_counter()
    await func(**kwargs)
    return time.perf_counter() - start


async def _get_average_latency(func, iters: int = 100, **kwargs):
    latency = []
    for _ in range(iters):
        latency.append(await _get_latency(func, **kwargs))
    return {"min": min(latency), "max": max(latency), "mean": mean(latency)}


async def _get_results(backend: Backend, key: str, iters=100):
    return await _get_average_latency(backend.get, key=key, iters=iters)


async def _set_results(backend: Backend, key: str, iters=100):
    return await _get_average_latency(backend.set, key=key, value="1", expire=10, iters=iters)


async def check_speed(backend: Backend, iters: int = 100):
    key = f"check:{str(uuid.uuid4())}"
    return {
        "set": await _set_results(backend, key, iters=iters),
        "get": await _get_results(backend, key, iters=iters),
    }


async def get_size_of(backend: Backend, func):
    template = getattr(func, "_key_template", None)
    if template is None:
        raise Exception()
    return backend.get_size_match(template_to_pattern(template))


# async def usage(backend: Backend, func):
#     pass
