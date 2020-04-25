import asyncio
import re
import time
import uuid
from collections import Counter, defaultdict
from statistics import mean
from string import Formatter

from .backends.interface import Backend
from .key import get_templates_for, template_to_pattern


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
    coros = []
    templates = get_templates_for(func)
    for template in templates:
        coros.append(backend.get_size_match(template_to_pattern(template)))
    return zip(templates, await asyncio.gather(*coros))


class _ReFormatter(Formatter):
    def get_value(self, key, args, kwargs):
        try:
            return kwargs[key]
        except KeyError:
            return f"(P?<{key}>.*)"


class _NonKeyFormatter(Formatter):
    def get_value(self, key, args, kwargs):
        try:
            return kwargs[key]
        except KeyError:
            return f"{{{key}}}"


non_key_formatter = _NonKeyFormatter()


def _create_reader(template: str, max_events):
    pattern = re.compile(_ReFormatter().format(template).encode())
    get_counter = defaultdict(Counter)
    set_counter = defaultdict(Counter)

    def reader(cmd: bytes, key: bytes):
        counter = get_counter if cmd == b"get" else set_counter
        reader._max_events -= 1
        match = pattern.match(key)
        if match:
            for name, value in match.groupdict().items():
                counter[name][value] += 1
        if reader._max_events < 0:
            raise StopIteration()

    reader._max_events = max_events
    return reader, (get_counter, set_counter)


async def usage(backend: Backend, func, max_events=10, **values):
    templates = get_templates_for(func)
    if not templates:
        raise ValueError("Function have no associate cache key")
    coros = []
    counters = []
    for template in templates:
        pattern = template_to_pattern(template, **values)
        reader_template = non_key_formatter.format(template, **values)
        reader, counter = _create_reader(reader_template, max_events=max_events)
        coros.append(backend.listen(pattern, "get", "set", reader=reader))
        counters.append(counter)
    if coros:
        await asyncio.gather(*coros, return_exceptions=True)
        return counters
