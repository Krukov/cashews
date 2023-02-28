import asyncio
from unittest.mock import Mock

import pytest

from cashews import Cache

pytestmark = pytest.mark.asyncio


async def test_iterator(cache: Cache):
    chunks = [b"a", b"b", b"c", b"d"]
    call = Mock()

    @cache.iterator(ttl=10, key="iterator")
    async def func():
        for chunk in chunks:
            call()
            await asyncio.sleep(0)
            yield chunk

    i = 0
    async for chunk in func():
        assert chunk == chunks[i]
        i += 1
    assert call.call_count == 4

    call.reset_mock()

    i = 0
    async for chunk in func():
        assert chunk == chunks[i]
        i += 1

    assert call.call_count == 0
