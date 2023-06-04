from unittest.mock import Mock

import pytest

from cashews import Cache, only_exceptions, with_exceptions

pytestmark = pytest.mark.asyncio
EXPIRE = 0.02


class CustomError(Exception):
    pass


async def test_cache_with_exceptions(cache: Cache):
    mock = Mock()

    @cache(ttl=EXPIRE, condition=with_exceptions(CustomError))
    async def func(raise_exception=None):
        if raise_exception:
            mock()
            raise raise_exception
        mock()
        return b"ok"

    assert await func() == b"ok"
    assert await func() == b"ok"

    with pytest.raises(CustomError):
        await func(CustomError())

    with pytest.raises(CustomError):
        await func(CustomError())

    with pytest.raises(Exception):
        await func(Exception())

    assert mock.call_count == 3


async def test_cache_only_exceptions(cache: Cache):
    mock = Mock()

    @cache(ttl=EXPIRE, condition=only_exceptions(CustomError))
    async def func(raise_exception=None):
        if raise_exception:
            mock()
            raise raise_exception
        mock()
        return b"ok"

    assert await func() == b"ok"
    assert await func() == b"ok"

    with pytest.raises(CustomError):
        await func(CustomError())

    with pytest.raises(CustomError):
        await func(CustomError())

    assert mock.call_count == 3


async def test_cache_early_exceptions(cache: Cache):
    mock = Mock()

    @cache.early(ttl=EXPIRE, condition=only_exceptions(CustomError))
    async def func(raise_exception=None):
        if raise_exception:
            mock()
            raise raise_exception

    with pytest.raises(CustomError):
        await func(CustomError())

    with pytest.raises(CustomError):
        await func(CustomError())

    mock.assert_called_once()


async def test_cache_hit_exceptions(cache: Cache):
    mock = Mock()

    @cache.hit(ttl=EXPIRE, cache_hits=10, condition=only_exceptions(CustomError))
    async def func(raise_exception=None):
        if raise_exception:
            mock()
            raise raise_exception

    with pytest.raises(CustomError):
        await func(CustomError())

    with pytest.raises(CustomError):
        await func(CustomError())

    mock.assert_called_once()
