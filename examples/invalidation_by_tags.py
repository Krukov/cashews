import asyncio
import random

from cashews import cache

cache.setup("redis://", client_side=True)


@cache(ttl="1h", tags=["items", "user_data:{user_id}"])
async def get_items(user_id: int):
    return [f"{user_id}_{random.randint(1, 10)}" for i in range(10)]


@cache(ttl="1h", tags=["products", "user_data:{user_id}"])
async def get_products(user_id: int):
    return [f"{user_id}_{random.randint(1, 10)}" for i in range(10)]


FIRST_USER = 1
SECOND_USER = 2


async def main():
    first_user_items = await get_items(FIRST_USER)
    first_user_products = await get_products(FIRST_USER)
    second_user_items = await get_items(SECOND_USER)
    second_user_products = await get_products(SECOND_USER)

    # check that results were cached
    assert await get_items(FIRST_USER) == first_user_items
    assert await get_products(FIRST_USER) == first_user_products
    assert await get_items(SECOND_USER) == second_user_items
    assert await get_products(SECOND_USER) == second_user_products

    # invalidate cache first user
    await cache.delete_tags(f"user_data:{FIRST_USER}")

    assert await get_items(FIRST_USER) != first_user_items
    assert await get_products(FIRST_USER) != first_user_products
    assert await get_items(SECOND_USER) == second_user_items
    assert await get_products(SECOND_USER) == second_user_products

    # invalidate the whole cache for two functions
    await cache.delete_tags("products", "items")
    assert await get_items(FIRST_USER) != first_user_items
    assert await get_products(FIRST_USER) != first_user_products
    assert await get_items(SECOND_USER) != second_user_items
    assert await get_products(SECOND_USER) != second_user_products


if __name__ == "__main__":
    asyncio.run(main())
