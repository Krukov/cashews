import asyncio
import socket
import logging

from aioredis import Redis, ConnectionError


logger = logging.getLogger(__name__)


class SafeRedis(Redis):

    def __init__(self, *args, **kwargs):
        self._safe = False
        super().__init__(*args, **kwargs)

    def set_safe(self):
        self._safe = True

    async def execute_command(self, command, *args, **kwargs):
        try:
            return await super().execute_command(command, *args, **kwargs)
        except (ConnectionError, socket.gaierror, OSError, asyncio.TimeoutError):
            if not self._safe or command.lower() == "ping":
                raise
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
            if self._safe:
                return self
            raise

    def client(self):
        _client = super().client()
        if self._safe:
            _client.set_safe()
        return _client

    __aenter__ = initialize