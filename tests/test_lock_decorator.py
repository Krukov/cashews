import asyncio
from unittest.mock import Mock

import pytest

from cashews import decorators
from cashews.backends.memory import Memory

pytestmark = pytest.mark.asyncio


async def test_lock_cache_parallel(cache):
    mock = Mock()

    @cache.locked(key="key", wait=False, ttl=10)
    async def func():
        await asyncio.sleep(0.1)
        mock()

    for _ in range(2):
        await asyncio.gather(*[func() for _ in range(10)], return_exceptions=True)

    assert mock.call_count == 2


async def test_lock_cache_parallel_with_ttl(cache):
    mock = Mock()

    @cache.locked(key="key", ttl=1, wait=True)
    async def func():
        await asyncio.sleep(0.01)
        mock()

    for _ in range(2):
        await asyncio.gather(*[func() for _ in range(10)])

    assert mock.call_count == 20


async def test_lock_cache_iterator(cache):
    mock = Mock()
    chunks = range(10)

    @cache.locked(key="key", ttl=10)
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

    @decorators.locked(backend, key="key")
    async def func(resp=b"ok"):
        await asyncio.sleep(0.01)
        return resp

    with pytest.raises(Exception):
        await asyncio.gather(*[func() for _ in range(10)])


async def test_thunder_protection():
    mock = Mock()

    @decorators.thunder_protection(key="key")
    async def func(resp=b"ok"):
        await asyncio.sleep(0.01)
        mock()
        return resp

    for _ in range(2):
        await asyncio.gather(*[func() for _ in range(10)])

    assert mock.call_count == 2
