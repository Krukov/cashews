import asyncio

from cashews import NOT_NONE, cache

cache.setup("redis://")


@cache(ttl="10m", condition=NOT_NONE)
async def cache_not_none(foo):
    await asyncio.sleep(0.1)
    return foo if foo < 100 else None


def _only_none_foo_more_1000(result, args, kwargs, key=None) -> bool:
    return result is None and args[0] > 1000


@cache(ttl="10m", condition=_only_none_foo_more_1000)
async def cache_only_none_more_1000(foo):
    await asyncio.sleep(0.1)
    return foo if foo < 100 else None


@cache(ttl="10m", time_condition="1s")
async def time_condition(foo):
    await asyncio.sleep(foo)
    return f"sleep {foo}"


async def main():
    print(await cache_not_none(101))  # no cache
    print(await cache_not_none(10))  # cache

    print(await cache_only_none_more_1000(10))  # no cache
    print(await cache_only_none_more_1000(101))  # no cache
    print(await cache_only_none_more_1000(1001))  # cache

    print(await time_condition(0.1))  # no cache
    print(await time_condition(2))  # cache


if __name__ == "__main__":
    asyncio.run(main())
