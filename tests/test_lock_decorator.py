import asyncio
from unittest.mock import Mock

import pytest

from cashews import decorators
from cashews.backends.memory import Memory

pytestmark = pytest.mark.asyncio


async def test_lock_cache_parallel(cache):
    mock = Mock()

    @cache.locked(key="key", step=0.01)
    async def func():
        await asyncio.sleep(0.1)
        mock()

    for _ in range(2):
        await asyncio.gather(*[func() for _ in range(10)], return_exceptions=True)

    assert mock.call_count == 2


async def test_lock_cache_parallel_with_ttl(cache):
    mock = Mock()

    @cache.locked(key="key", step=0.01, ttl=1)
    async def func():
        await asyncio.sleep(0.1)
        mock()

    for _ in range(2):
        await asyncio.gather(*[func() for _ in range(10)])

    assert mock.call_count == 20


async def test_lock_cache_parallel_with_min(cache):
    mock = Mock()

    @cache.locked(key="key", step=0.01, min_wait_time=1)
    async def func():
        await asyncio.sleep(0.1)
        mock()

    for _ in range(2):
        await asyncio.gather(*[func() for _ in range(10)])

    assert mock.call_count == 20


async def test_lock_cache_parallel_ttl(cache):
    mock = Mock()

    @cache.locked(key="key", step=0.01, ttl=0.1)
    async def func(resp=b"ok"):
        await asyncio.sleep(0.01)
        mock(resp)
        return resp

    for _ in range(4):
        await asyncio.gather(*[func() for _ in range(10)], return_exceptions=True)

    assert mock.call_count == 40


async def test_lock_cache_iterator(cache):
    mock = Mock()
    chunks = range(10)

    @cache.locked(key="key", step=0.01)
    async def func():
        for chunk in chunks:
            mock()
            await asyncio.sleep(0)
            yield chunk

    async for _ in func():
        pass
    assert mock.call_count == 10


async def test_lock_cache_broken_backend():
    class BrokenMemoryBackend(Memory):
        async def ping(self, message=None) -> bytes:
            raise Exception("broken")

    backend = Mock(wraps=BrokenMemoryBackend())

    @decorators.locked(backend, key="key", step=0.01)
    async def func(resp=b"ok"):
        await asyncio.sleep(0.01)
        return resp

    with pytest.raises(Exception):
        await asyncio.gather(*[func() for _ in range(10)])
