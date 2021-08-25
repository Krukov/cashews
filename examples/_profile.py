import sys
import random
sys.path.append("/Users/dmitry.kryukov/Projects/my/cashews")
import asyncio
from datetime import timedelta

from cashews import default_formatter, cache, context_cache_detect

from pyinstrument import Profiler

import uvloop



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


cache.setup("disk://?directory=/tmp/cache")


# @profile(precision=4)
@cache.cache(ttl=timedelta(minutes=20), key="mul:{a:human}")
async def example(a):
    return {"1": "2"}
#
# from guppy import hpy
# hp = hpy()


async def main():
    await cache.init()
    p = Profiler(async_mode='disabled')
    with p:
            # before = hp.heap()
            for _ in range(100000):
                with context_cache_detect:
                    await asyncio.gather(
                        example(random.randint(10, 1000)),
                        example(random.randint(10, 10000)),
                        example(random.randint(10, 1000)),
                    )
            # after = hp.heap()

            # lo = after - before
            # import ipdb; ipdb.set_trace()
    p.print()

# uvloop.install()
asyncio.run(main())

