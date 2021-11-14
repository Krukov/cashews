
_AIOREDIS_VERSION_1 = "1"
try:
    from aioredis import BlockingConnectionPool, ConnectionError as RedisConnectionError, Redis
    _AIOREDIS = "2"

except ImportError:
    from aioredis import Redis, create_pool as BlockingConnectionPool, RedisError as RedisConnectionError
    _AIOREDIS = _AIOREDIS_VERSION_1

AIOREDIS_IS_VERSION_1 = _AIOREDIS == _AIOREDIS_VERSION_1


__all__ = ["AIOREDIS_IS_VERSION_1", "Redis", "BlockingConnectionPool", "RedisConnectionError"]