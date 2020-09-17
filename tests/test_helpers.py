from unittest.mock import Mock

import pytest
from cashews.backends.interface import Backend
from cashews.utils import check_speed


@pytest.mark.asyncio
async def test_speed_test():
    backend = Mock(wraps=Backend())

    result = await check_speed(backend, iters=10)

    assert len(result) == 2
    assert "get" in result
    assert "set" in result
    assert backend.set.call_count == 10
    assert backend.get.call_count == 10
