import asyncio

from cashews import cache

cache.setup("redis://")


@cache(ttl="10m")
async def long_running_function(foo):
    await asyncio.sleep(0.1)
    return foo


@cache.hit(ttl="10m", cache_hits=1000)
async def long_running_function_hit(foo):
    await asyncio.sleep(0.1)
    return foo


@cache.early(ttl="10m", early_ttl="5m")
async def long_running_function_early(foo):
    await asyncio.sleep(0.1)
    return foo


@cache.soft(ttl="10m", soft_ttl="5m")
async def long_running_function_soft(foo):
    await asyncio.sleep(0.1)
    return foo


@cache.failover(ttl="10m")
async def long_running_function_fail(foo):
    await asyncio.sleep(0.1)
    return foo


async def main():
    await cache.clear()
    print(await long_running_function("simple"))
    print(await long_running_function_hit("hit"))
    print(await long_running_function_early("early"))
    print(await long_running_function_soft("soft"))
    print(await long_running_function_fail("fail over"))


if __name__ == "__main__":
    asyncio.run(main())
