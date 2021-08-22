import sys
import random
sys.path.append("/Users/dmitry.kryukov/Projects/my/cashews")
import asyncio
from datetime import timedelta

from cashews import default_formatter, cache

# import yappi
from pyinstrument import Profiler


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


@default_formatter.register("human")
def _human(value):
    res = ""
    for char in value:
        if res:
            res += "-"
        res += INT_TO_STR_MAP.get(char)
    return res


cache.setup("disk://")


@cache.cache(ttl=timedelta(minutes=20), key="mul:{a:human}")
async def example(a):
    return [{"1": "a" * i for _ in range(i)} for i in range(a)]


async def main():
    # await cache.init()
    p = Profiler(async_mode='disabled')
    with p:
        for _ in range(10000):
            await asyncio.gather(
                example(random.randint(10, 100)),
                example(random.randint(10, 100)),
                example(random.randint(10, 100)),
            )
    p.print()

asyncio.run(main())

