from unittest.mock import Mock

from cashews.commands import Command
from cashews.wrapper import Cache


async def test_with_callback_on_set(cache: Cache):
    call = Mock()

    with cache.callback(callback=call, cmd=Command.SET):
        await cache.set("test", "value")

    call.assert_called_once_with("test", result=True)


async def test_with_callback_on_set_skip_get(cache: Cache):
    call = Mock()

    with cache.callback(callback=call, cmd=Command.SET):
        await cache.get(
            "test",
        )

    call.assert_not_called()
