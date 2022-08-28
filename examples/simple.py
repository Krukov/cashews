import asyncio
from datetime import timedelta  # noqa: F401
from decimal import Decimal

from cashews import CacheDetect, add_prefix, cache, context_cache_detect  # noqa: F401


async def main():
    await cache.clear()

    @cache(ttl=1000)
    async def test(key):
        return await cache.incr(key), Decimal(10), b"d"

    with context_cache_detect as detect:
        assert not detect.keys
        print(await cache.incr("key"), "== 1")
        print(await test("key"), "== 2")
        await asyncio.sleep(0)
        print(detect.keys)
        print(await test("key"), "== 2")
        print(detect.keys)
    detect.clear()

    print(await test("key"), "== 2")
    print(detect.keys)

    with cache.disabling("get", "set"):
        print(await test("key"), "disable")
    print(await test("key"), "from cache 2")

    # test.disable()
    print(await test.direct("key"))

    # # return
    # async with cache.lock("lock", expire=10):
    #     await cache.clear()
    #     await cache.set("key", value={"any": True}, expire=timedelta(minutes=1))  # -> bool
    #     await cache.get("key")  # -> Any
    #     await cache.incr("inr_key")  # -> int
    #     await cache.expire("key", timeout=timedelta(hours=10))
    #     await cache.delete("key")
    #     print(await cache.ping())  # -> bytes
    #
    # await cache.set_lock("key", value="value", expire=60)  # -> bool
    # print(await cache.is_locked("key", wait=10, step=1))  # -> bool
    # await cache.unlock("key", "value")  # -> bool


if __name__ == "__main__":
    # cache.setup("mem://", hooks=[prefix])
    cache.setup("redis://0.0.0.0/2?hash_key=s3243fedg", middlewares=(add_prefix("test:"),))
    asyncio.run(main())
