import asyncio
from unittest.mock import Mock

import pytest

from cashews import Cache, with_exceptions


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


class MyException(Exception):
    pass


async def test_iterator_error_with_cond(cache: Cache):
    call = Mock(side_effect=["a", "b", MyException(), "c", "d"])

    @cache.iterator(ttl=10, key="iterator", condition=with_exceptions(MyException))
    async def func():
        while True:
            try:
                yield call()
            except StopIteration:
                return
            await asyncio.sleep(0)

    full = ""
    with pytest.raises(MyException):
        async for chunk in func():
            assert chunk
            full += chunk

    assert full == "ab"
    assert call.call_count == 3
    call.reset_mock()

    with pytest.raises(MyException):
        async for chunk in func():
            assert chunk
            full += chunk

    assert full == "abab"
    assert call.call_count == 0


async def test_iterator_error(cache: Cache):
    call = Mock(side_effect=["a", MyException(), "c"])

    @cache.iterator(ttl=10, key="iterator")
    async def func():
        while True:
            try:
                yield call()
            except StopIteration:
                return
            await asyncio.sleep(0)

    with pytest.raises(MyException):
        async for chunk in func():
            assert chunk == "a"

    assert call.call_count == 2  # return a and raise error

    async for chunk in func():
        assert chunk == "c"

    assert call.call_count == 4  # return a, raise error + return c and stopIteration

    async for chunk in func():
        assert chunk == "c"

    assert call.call_count == 4
