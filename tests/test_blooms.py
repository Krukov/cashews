import asyncio
import uuid
from functools import partial

import pytest

from cashews.backends.memory import Memory
from cashews.decorators.bloom import bloom as _bloom, bloom_count, params_for, _count_k

pytestmark = pytest.mark.asyncio


@pytest.fixture(name="bloom")
async def __bloom():
    return partial(_bloom, backend=Memory())


@pytest.mark.parametrize("n", list(range(10, 300, 20)))
async def test_bloom_simple(bloom, n):

    @bloom(name="name:{k}", index_size=int(n / 10), number_of_hashes=_count_k(n / 10, n))
    async def func(k):
        return k > (n / 2)

    for i in range(n):
        await func.set(i)
        assert await func(i) is (i > (n / 2))

