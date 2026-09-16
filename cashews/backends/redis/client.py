import asyncio
import logging
import socket
from typing import Any

from redis.asyncio import Redis as _Redis
from redis.asyncio import RedisCluster as _RedisCluster
from redis.asyncio.client import Pipeline
from redis.exceptions import NoScriptError, RedisClusterException
from redis.exceptions import RedisError as RedisConnectionError

from cashews.exceptions import CacheBackendInteractionError

logger = logging.getLogger(__name__)

_redis_connection_errors = (
    RedisConnectionError,
    RedisClusterException,
    socket.gaierror,
    OSError,
    asyncio.TimeoutError,
)


class Redis(_Redis):
    async def execute_command(self, command, *args: Any, **kwargs: Any):
        try:
            return await super().execute_command(command, *args, **kwargs)
        except NoScriptError:
            # used by register_script functionality
            # if we do not reraise it, than a Script wrapper will not work as expect
            raise
        except _redis_connection_errors as exp:
            raise CacheBackendInteractionError() from exp


class RedisCluster(_RedisCluster):
    async def execute_command(self, *args: Any, **kwargs: Any):
        try:
            return await super().execute_command(*args, **kwargs)
        except NoScriptError:
            # used by register_script functionality
            # if we do not reraise it, than a Script wrapper will not work as expect
            raise
        except _redis_connection_errors as exp:
            raise CacheBackendInteractionError() from exp


class SafeRedis(_Redis):
    async def execute_command(self, command, *args: Any, **kwargs: Any):
        try:
            return await super().execute_command(command, *args, **kwargs)
        except NoScriptError:
            # used by register_script functionality
            # if we do not reraise it, than a Script wrapper will not work as expect
            raise
        except _redis_connection_errors as exp:
            if command.lower() == "ping":
                raise CacheBackendInteractionError() from exp
            logger.error("redis: can not execute command: %s", command, exc_info=True)
            if command.lower() in ["unlink", "del", "memory", "ttl"]:
                return 0
            if command.lower() == "scan":
                return [0, []]
            return None

    async def initialize(self):
        try:
            return await super().initialize()
        except _redis_connection_errors:
            logger.error("redis: can not initialize cache", exc_info=True)
            return self

    __aenter__ = initialize


class SafeRedisCluster(_RedisCluster):
    async def execute_command(self, *args: Any, **kwargs: Any):
        try:
            return await super().execute_command(*args, **kwargs)
        except NoScriptError:
            # used by register_script functionality
            # if we do not reraise it, than a Script wrapper will not work as expect
            raise
        except _redis_connection_errors as exp:
            command = args[0] if args else ""
            if command.lower() == "ping":
                raise CacheBackendInteractionError() from exp
            logger.error("redis: can not execute command: %s", command, exc_info=True)
            if command.lower() in ["unlink", "del", "memory", "ttl"]:
                return 0
            if command.lower() == "scan":
                return [0, []]
            return None

    async def initialize(self):
        try:
            return await super().initialize()
        except _redis_connection_errors:
            logger.error("redis: can not initialize cache", exc_info=True)
            return self

    __aenter__ = initialize


class SafePipeline(Pipeline):
    async def execute(self, raise_on_error=False):
        try:
            await super().execute(raise_on_error)
        except RedisConnectionError:
            logger.error("redis: can not execute pipeline", exc_info=True)
