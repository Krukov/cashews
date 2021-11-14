import asyncio
import logging
import socket

from .compat import AIOREDIS_IS_VERSION_1, Redis, RedisConnectionError

logger = logging.getLogger(__name__)


class SafeRedis(Redis):
    if AIOREDIS_IS_VERSION_1:

        async def execute(self, command, *args, **kwargs):
            try:
                return await super().execute(command, *args, **kwargs)
            except (RedisConnectionError, socket.gaierror, OSError, asyncio.TimeoutError, AttributeError):
                logger.error("redis: can not execute command: %s", command, exc_info=True)
                if command.lower() in [b"unlink", b"del", b"memory", b"ttl"]:
                    return 0
                if command.lower() == b"scan":
                    return [0, []]
                return None

    else:

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
