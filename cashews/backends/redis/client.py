import asyncio
import logging
import socket

try:
    from redis.asyncio import Redis
    from redis.exceptions import ConnectionError as RedisConnectionError
except ImportError:
    from aioredis import Redis
    from aioredis import RedisError as RedisConnectionError


logger = logging.getLogger(__name__)


class SafeRedis(Redis):
    async def execute_command(self, command, *args, **kwargs):
        try:
            return await super().execute_command(command, *args, **kwargs)
        except (RedisConnectionError, socket.gaierror, OSError, asyncio.TimeoutError):
            logger.error("redis: can not execute command: %s", command, exc_info=True)
            if command.lower() in ["unlink", "del", "memory", "ttl"]:
                return 0
            if command.lower() == "scan":
                return [0, []]
            return None

    async def initialize(self):
        try:
            return await super().initialize()
        except Exception:
            return self

    __aenter__ = initialize
