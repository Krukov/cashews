_AIOREDIS_VERSION_1 = "1"
try:
    from aioredis import BlockingConnectionPool
    from aioredis import ConnectionError as RedisConnectionError
    from aioredis import Redis

    _AIOREDIS = "2"

except ImportError:
    from aioredis import Redis
    from aioredis import RedisError as RedisConnectionError
    from aioredis import create_pool as BlockingConnectionPool

    _AIOREDIS = _AIOREDIS_VERSION_1

AIOREDIS_IS_VERSION_1 = _AIOREDIS == _AIOREDIS_VERSION_1


__all__ = ["AIOREDIS_IS_VERSION_1", "Redis", "BlockingConnectionPool", "RedisConnectionError"]
