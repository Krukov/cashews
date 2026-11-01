from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("redis.asyncio")

from redis.asyncio import RedisCluster
from redis.exceptions import RedisClusterException

from cashews.backends.redis.client import SafeRedisCluster

pytestmark = [pytest.mark.asyncio, pytest.mark.redis]


@pytest.mark.parametrize("connection_error", [False, True])
async def test_safe_cluster_initialize_forwards_arguments(connection_error):
    client = SafeRedisCluster(host="localhost", port=6379)
    startup_nodes = [("localhost", 6380)]
    initialize = AsyncMock(return_value=client)
    if connection_error:
        initialize.side_effect = RedisClusterException("cluster is unavailable")

    with patch.object(RedisCluster, "initialize", initialize):
        result = await client.initialize(startup_nodes, last_failed_node_name="localhost:6379")

    assert result is client
    initialize.assert_awaited_once_with(startup_nodes, last_failed_node_name="localhost:6379")
