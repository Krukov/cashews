_AIOREDIS_VERSION_1 = "1"
try:
    from redis.asyncio import BlockingConnectionPool, Redis
    from redis.exceptions import ConnectionError as RedisConnectionError

    _AIOREDIS = "2"  # Technically, not "aioredis" anymore (just "redis"). Keep this name as long as we support "aioredis".

except ImportError:
    from aioredis import Redis
    from aioredis import RedisError as RedisConnectionError
    from aioredis import create_pool as BlockingConnectionPool

    _AIOREDIS = _AIOREDIS_VERSION_1

AIOREDIS_IS_VERSION_1 = _AIOREDIS == _AIOREDIS_VERSION_1


__all__ = ["AIOREDIS_IS_VERSION_1", "Redis", "BlockingConnectionPool", "RedisConnectionError"]
