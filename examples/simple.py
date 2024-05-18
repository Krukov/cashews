import asyncio
from decimal import Decimal

from cashews import Command, add_prefix, all_keys_lower, cache  # noqa: F401

cache.setup(
    "redis://0.0.0.0/2",
    client_name=None,
    hash_key="test",
    digestmod="md5",
    middlewares=(add_prefix("test:"), all_keys_lower()),
)


async def basic():
    await cache.clear()

    await cache.set("key", 1)
    assert await cache.get("key") == 1
    await cache.set("key1", value={"any": True}, expire="1m")
    print(await cache.get_or_set("key200", default=lambda: "test"))
    print(await cache.get_or_set("key10", default="test"))

    await cache.set_many({"key2": "test", "key3": Decimal("10.1")}, expire="1m")
    print("Get: ", await cache.get("key1"))  # -> Any

    async for key in cache.scan("key*"):
        print("Scan:", key)  # -> Any

    async for key, value in cache.get_match("key*"):
        print("Get match:", key, value)  # -> Any

    print("Get many:", await cache.get_many("key2", "key3"))  # -> Any
    print("Incr:", await cache.incr("inr_key"))  # -> int

    await cache.expire("key1", timeout="1h")
    print("Expire: ", await cache.get_expire("key1"))

    await cache.delete("key1")
    await cache.delete_many("key2", "key3")

    async with cache.lock("lock", expire="1m"):
        print("Locked: ", await cache.is_locked("lock"))

    print("Ping: ", await cache.ping())  # -> bytes
    print("Count: ", await cache.get_keys_count())

    await cache.close()


if __name__ == "__main__":
    asyncio.run(basic())
