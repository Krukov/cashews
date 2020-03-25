import asyncio
from decimal import Decimal
from datetime import timedelta
from cashews import cache, add_prefix


async def main(cache):

    await cache.clear()

    @cache.cache(ttl=10)
    async def test(key):
        return await cache.incr(key), Decimal("100.01"), u"привет"

    print(await cache.incr("key"), "== 1")
    print(await test("key"), "== 2")

    async with cache.lock("lock", expire=10):
        await cache.clear()
        await cache.set("key", value={"any": True}, expire=timedelta(minutes=1))  # -> bool
        await cache.get("key")  # -> Any
        await cache.incr("inr_key")  # -> int
        await cache.expire("key", timeout=timedelta(hours=10))
        await cache.delete("key")
        print(await cache.ping())  # -> bytes

    await cache.set_lock("key", value="value", expire=60)  # -> bool
    print(await cache.is_locked("key", wait=10, step=1))  # -> bool
    await cache.unlock("key", "value")  # -> bool



if __name__ == "__main__":
    # cache.setup("mem://", hooks=[prefix])
    cache.setup("redis://0.0.0.0/0?hash_key=s3243fedg", middlewares=(add_prefix("test:"), ))
    asyncio.run(main(cache))
