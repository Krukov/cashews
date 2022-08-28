import random
import sys

sys.path.append("/Users/dmitry.kryukov/Projects/my/cashews")
import asyncio
from datetime import timedelta

from pyinstrument import Profiler

from cashews import cache, default_formatter

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
def _human(value, upper=False):
    res = ""
    for char in value:
        if res:
            res += "-"
        res += INT_TO_STR_MAP.get(char)
    if upper:
        return res.upper()
    return res


# cache.setup("disk://?directory=/tmp/cache")
# cache.setup("mem://")
cache.setup("redis://?max_connections=5")


# @profile(precision=4)
@cache.failover(ttl=timedelta(minutes=20), key="mul:{a:human}:hit")
@cache.failover(ttl=timedelta(minutes=1), key="mul2:{a:hash}:hit")
# @cache(ttl=timedelta(minutes=20), key="mul:{a:human(true)}")
# @cache.hit(ttl=timedelta(minutes=20), key="mul:{a:human(true)}:hit", cache_hits=1000, update_after=900)
# @cache.hit(ttl=timedelta(minutes=20), key="mul:{a}:hit", cache_hits=1000, update_after=900)
# @cache.early(ttl=timedelta(minutes=20), key="mul:{a}:hit", early_ttl=timedelta(minutes=5))
# @cache.locked(ttl=timedelta(minutes=20), key="mul:{a:human(true)}:hit")
async def example(a):
    return {"1": "2"}


async def main():
    await cache.init()
    p = Profiler(async_mode="disabled")
    with p:
        for _ in range(10_000):
            await asyncio.gather(
                example(random.randint(10, 1000)),
                example(random.randint(10, 10000)),
                example(random.randint(10, 1000)),
            )
    p.print()


asyncio.run(main())
