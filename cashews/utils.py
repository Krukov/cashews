import gc
import sys
import time
import uuid
from statistics import mean

from .backends.interface import Backend


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


def _get_obj_size(obj) -> int:
    marked = {id(obj)}
    obj_q = [obj]
    size = 0

    while obj_q:
        size += sum(map(sys.getsizeof, obj_q))

        # Lookup all the object referred to by the object in obj_q.
        # See: https://docs.python.org/3.7/library/gc.html#gc.get_referents
        all_refr = ((id(o), o) for o in gc.get_referents(*obj_q))

        # Filter object that are already marked.
        # Using dict notation will prevent repeated objects.
        new_refr = {o_id: o for o_id, o in all_refr if o_id not in marked and not isinstance(o, type)}

        # The new obj_q will be the ones that were not marked,
        # and we will update marked with their ids so we will
        # not traverse them again.
        obj_q = new_refr.values()
        marked.update(new_refr.keys())

    return size
