import asyncio

from cashews import cache  # noqa: F401

cache.setup("redis://0.0.0.0/1")  # main
cache.setup("redis://0.0.0.0/2", prefix="users")
cache.setup("mem://", prefix="fail")


@cache.failover(ttl="10m")
@cache(ttl="10m", prefix="users")
async def long_running_function(foo):
    return await cache.get(foo)


async def main():
    await long_running_function("foo")


if __name__ == "__main__":
    asyncio.run(main())
