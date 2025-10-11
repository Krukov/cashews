import asyncio

from cashews import Cache


async def test_issue_382():
    cache = Cache()
    cache.setup("mem://")

    @cache(ttl=60, key="item:{item_id}")
    async def get_item(item_id: int):
        return f"item_{item_id}"

    def sync_get_item(item_id: int):
        return asyncio.run(get_item(item_id))

    await asyncio.get_running_loop().run_in_executor(None, sync_get_item, 123)
