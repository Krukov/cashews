import asyncio
from decimal import Decimal
from uuid import uuid4

from cashews import TransactionMode, cache

cache.setup("redis://", client_side=True)


@cache.transaction(TransactionMode.SERIALIZABLE)
@cache.cache(ttl="10s")
async def func(a):
    # await asyncio.sleep(0.01)
    return Decimal("10")


async def main():
    # async with cache.transaction(TransactionMode.SERIALIZABLE):
    await cache.init()
    for _ in range(5_000):
        await asyncio.gather(
            func(1),
            func(Decimal("102.2")),
            func(uuid4()),
        )


asyncio.run(main())
