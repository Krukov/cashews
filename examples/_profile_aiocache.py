import random
import asyncio


from pyinstrument import Profiler
from aiocache import cached, Cache
from aiocache.serializers import PickleSerializer


INT_TO_STR_MAP = {
    "0": "zero",
    "1": "one",
    "2": "two",
    "3": "three",
    "4": "four",
    "5": "five",
    "6": "six",
    "7": "seven",
    "8": "eight",
    "9": "nine",
}


def _human(func, a):
    res = ""
    for char in str(a):
        if res:
            res += "-"
        res += INT_TO_STR_MAP.get(char)
    return res


@cached(
    ttl=10, cache=Cache.REDIS, key_builder=_human, serializer=PickleSerializer())
async def example(a):
    return {"1": "2"}


async def main():
    p = Profiler(async_mode='disabled')
    with p:
            for _ in range(10_000):
                await asyncio.gather(
                    example(random.randint(10, 1000)),
                    example(random.randint(10, 10000)),
                    example(random.randint(10, 1000)),
                )
    p.print()

asyncio.run(main())

