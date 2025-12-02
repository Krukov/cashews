import asyncio

from cashews import Cache
from cashews.commands import Command


async def test_issue_382():
    cache = Cache()
    cache.setup("mem://")

    @cache(ttl=60, key="item:{item_id}")
    async def get_item(item_id: int):
        return f"item_{item_id}"

    def sync_get_item(item_id: int):
        return asyncio.run(get_item(item_id))

    await asyncio.get_running_loop().run_in_executor(None, sync_get_item, 123)


async def test_control_mixin_cross_context():
    """Test that ControlMixin's ContextVar works across different async contexts.

    This simulates the FastAPI lifespan pattern where cache.setup() is called in one
    context (lifespan) and cache operations are called in different contexts (HTTP requests).
    """
    cache = Cache()

    # Setup cache in one context (simulating FastAPI lifespan)
    cache.setup("mem://")
    cache.enable()  # This calls self.__disable.get() internally

    @cache(ttl=60, key="test:{value}")
    async def cached_func(value: int):
        return f"result_{value}"

    # Test in a different async context (simulating HTTP request)
    async def test_in_new_context():
        # This should not raise LookupError when checking if cache is enabled/disabled
        result = await cached_func(123)
        assert result == "result_123"

        # Test disable/enable in new context
        cache.disable(Command.SET)
        assert cache.is_disable(Command.SET)

        cache.enable(Command.SET)
        assert cache.is_enable(Command.SET)

        return result

    def sync_runner():
        return asyncio.run(test_in_new_context())

    # Run in executor with new event loop (different context)
    result = await asyncio.get_running_loop().run_in_executor(None, sync_runner)
    assert result == "result_123"


async def test_issue_397_tags_with_nested_attributes():
    """
    Regression test for tags with multiple nested attributes from same object.
    Issue #397: Tags fail when key template uses nested attributes like {data.first}:{data.second}
    Error was: re.error: redefinition of group name 'data' as group 2; was group 1
    """
    cache = Cache()
    cache.setup("mem://")

    class Data:
        def __init__(self, first, second):
            self.first = first
            self.second = second

    # This should NOT raise re.error during decoration
    @cache(ttl="5m", key="f:{data.first}:s:{data.second}", tags=["computed"])
    async def mult(data: Data):
        return data.first * data.second

    result = await mult(Data(3, 5))
    assert result == 15

    # Verify caching works
    cached_result = await mult(Data(3, 5))
    assert cached_result == 15

    # Verify tag deletion works
    await cache.delete_tags("computed")
