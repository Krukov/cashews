import asyncio
import contextlib
import random
import time
import uuid
from statistics import mean, pstdev

from cashews import Cache
from cashews.backends import client_side, diskcache, redis

# from aiocache import caches

prefix = str(uuid.uuid4())
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


def _key_static():
    return f"{prefix}:1"


def _key_random():
    return f"{prefix}:{random.randint(1, 1000)}"


def _big():
    return [{"name": f"name_{i}", "id": i} for i in range(300)]


async def _get_latency(func, *args, **kwargs) -> float:
    start = time.perf_counter()
    await func(*args, **kwargs)
    return time.perf_counter() - start


async def run(target, test, iters=1000):
    with contextlib.suppress(AttributeError, TypeError):
        await target.init()

    method, key_gen, _options = test
    _options = dict(_options)

    method = getattr(target, method)
    await target.clear()
    if _options.get("init"):
        value = _options.pop("value", "no_value")
        if callable(value):
            value = value()
        await target.set(_options.pop("init"), value)

    async def execute():
        options = dict(_options)
        key = key_gen()
        if options.get("set"):
            await target.set(key, options.pop("set", "no_value"))
        return await _get_latency(method, key, **options)

    latencies = []
    for _ in range(iters):
        latencies.append(await execute())
    print(
        "      max         ",
        "         mean        ",
        "      pstdev       ",
        len(latencies),
    )
    print(max(latencies), mean(latencies), pstdev(latencies))


# caches.set_config({
#     "default": {
#         'cache':    "aiocache.RedisCache",
#         'endpoint': "127.0.0.1",
#         'port':     6379,
#     },
#     'redis_pickle': {
#         'cache': "aiocache.RedisCache",
#         'endpoint': "127.0.0.1",
#         'port': 6379,
#         'serializer': {
#             'class': "aiocache.serializers.PickleSerializer"
#         },
#         'plugins': [
#             {'class': "aiocache.plugins.HitMissRatioPlugin"},
#             {'class': "aiocache.plugins.TimingPlugin"}
#         ]
#     }
# })

if __name__ == "__main__":
    choices = (
        input(
            """
    choose a backends
    1) aiocache simple
    2) aiocache pickle
    3) cashews hash
    4) cashews no hash
    5) cashews with client side
    6) cashews with client side wrapper
    """
        )
        or "2 3"
    )
    backends = {
        # 1: ("aiocache simple", caches.get("default")),
        # 2: ("aiocache pickle", caches.get("redis_pickle")),
        2: ("cashews disk", diskcache.DiskCache()),
        3: ("cashews hash", redis.Redis("redis://localhost/", hash_key=b"f34feyhg;s2")),
        4: ("cashews no hash", redis.Redis("redis://localhost/", hash_key=None)),
        5: (
            "cashews with client side",
            client_side.BcastClientSide("redis://localhost/", hash_key=None),
        ),
        6: (
            "cashews full",
            Cache().setup("redis://localhost/", hash_key="test", client_side=True),
        ),
        7: (
            "cashews full disk",
            Cache().setup(
                "redis://localhost/",
                hash_key="test",
                client_side=True,
                local_cache=diskcache.DiskCache(),
            ),
        ),
    }
    targets = []
    for choice in choices.split():
        targets.append(backends.get(int(choice)))

    choices = (
        input(
            """
        choose a test
        1) get static key
        2) get random key
        3) get miss static key
        4) get miss random key
        5) set static key
        6) set random key
        7) incr static key
        8) incr random key
        9) del static key
        10) del random key
        11) get big object
    """
        )
        or "1 2 3 4 5 11"
    )
    _tests = {
        1: ("get", lambda: "test", {"init": "test"}),
        2: ("get", _key_random, {"set": "test"}),
        3: ("get", _key_static, {}),
        4: ("get", _key_random, {}),
        5: ("set", _key_static, {"value": list(range(100))}),
        6: ("set", _key_random, {"value": b"simple"}),
        7: ("incr", _key_static, {}),
        8: ("incr", _key_random, {}),
        9: ("delete", _key_static, {}),
        10: ("delete", _key_random, {}),
        11: ("get", _key_static, {"init": _key_static(), "value": _big}),
    }
    tests = []
    for choice in choices.split():
        tests.append((choice, _tests.get(int(choice))))

    iters = int(input("Iters: ") or "10000")

    for test in tests:
        print("=" * 100)
        print(f"TEST = {test}")
        for target in targets:
            print(f"-----CACHE NAME = {target[0]}-----")
            loop.run_until_complete(run(target[1], test[1], iters=iters))
