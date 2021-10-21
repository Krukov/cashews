import asyncio
import logging
import socket

from aioredis import ConnectionError, Redis

logger = logging.getLogger(__name__)


class SafeRedis(Redis):
    async def execute_command(self, command, *args, **kwargs):
        try:
            return await super().execute_command(command, *args, **kwargs)
        except (ConnectionError, socket.gaierror, OSError, asyncio.TimeoutError):
            logger.error("Redis down on command %s", command)
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
