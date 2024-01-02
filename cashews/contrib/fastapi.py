from __future__ import annotations

import contextlib
from contextlib import nullcontext
from contextvars import ContextVar
from hashlib import blake2s
from typing import Any, ContextManager, Sequence

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from cashews import Cache, Command, cache, invalidate_further
from cashews._typing import TTL

_CACHE_MAX_AGE: ContextVar[int] = ContextVar("cache_control_max_age")

_CACHE_CONTROL_HEADER = "Cache-Control"
_AGE_HEADER = "Age"
_ETAG_HEADER = "ETag"
_IF_NOT_MATCH_HEADER = "If-None-Match"
_CLEAR_CACHE_HEADER = "Clear-Site-Data"

_NO_CACHE = "no-cache"  # disable GET
_NO_STORE = "no-store"  # disable GET AND SET
_MAX_AGE = "max-age="
_ONLY_IF_CACHED = "only-if-cached"
_PRIVATE = "private"
_PUBLIC = "public"

_CLEAR_CACHE_HEADER_VALUE = "cache"
__all__ = ["cache_control_ttl", "CacheRequestControlMiddleware", "CacheEtagMiddleware", "CacheDeleteMiddleware"]


def cache_control_ttl(default: TTL):
    def _ttl(*args, **kwargs):
        return _CACHE_MAX_AGE.get(default)

    return _ttl


class CacheRequestControlMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, cache_instance: Cache = cache, methods: Sequence[str] = ("get",), private=True):
        self._private = private
        self._cache = cache_instance
        self._methods = methods
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        context: ContextManager = nullcontext()
        cache_control_value = request.headers.get(_CACHE_CONTROL_HEADER)
        if request.method.lower() not in self._methods:
            return await call_next(request)
        to_disable = self._to_disable(cache_control_value)
        if to_disable:
            context = self._cache.disabling(*to_disable)
        with context, self.max_age(cache_control_value), self._cache.detect as detector:
            response = await call_next(request)
            calls = detector.calls_list
            if calls:
                key, _ = calls[0]
                expire = await self._cache.get_expire(key)
                if expire > 0:
                    response.headers[
                        _CACHE_CONTROL_HEADER
                    ] = f"{_PRIVATE if self._private else _PUBLIC}, {_MAX_AGE}{expire}"
                    response.headers[_AGE_HEADER] = f"{expire}"
            else:
                response.headers[_CACHE_CONTROL_HEADER] = _NO_CACHE
            return response

    @contextlib.contextmanager
    def max_age(self, cache_control_value: str | None):
        if not cache_control_value:
            yield
            return
        _max_age = self._get_max_age(cache_control_value)
        reset_token = None
        if _max_age:
            reset_token = _CACHE_MAX_AGE.set(_max_age)
        try:
            yield
        finally:
            if reset_token:
                _CACHE_MAX_AGE.reset(reset_token)

    def _to_disable(self, cache_control_value: str | None) -> tuple[Command, ...]:
        if cache_control_value == _NO_CACHE:
            return (Command.GET,)
        if cache_control_value == _NO_STORE:
            return Command.GET, Command.SET
        if cache_control_value and self._get_max_age(cache_control_value) == 0:
            return Command.GET, Command.SET
        return tuple()

    @staticmethod
    def _get_max_age(cache_control_value: str) -> int | None:
        if _MAX_AGE not in cache_control_value:
            return None
        for cc_value_item in cache_control_value.split(","):
            cc_value_item = cc_value_item.strip()
            try:
                key, value = cc_value_item.split("=")
                if key == _MAX_AGE[:-1]:
                    return int(value)
            except (ValueError, TypeError):
                continue
        return None


class CacheEtagMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, cache_instance: Cache = cache):
        self._cache = cache_instance
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
        etag = request.headers.get(_IF_NOT_MATCH_HEADER)
        if etag and await self._cache.exists(etag):
            return Response(status_code=304)

        set_key = None

        def set_callback(key: str, result: Any):
            nonlocal set_key
            set_key = key

        with self._cache.detect as detector, self._cache.callback(set_callback, cmd=Command.SET):
            response = await call_next(request)
            calls = detector.calls_list
            if not calls:
                if set_key:
                    response.headers[_ETAG_HEADER] = await self._get_etag(set_key)
                return response

            key, _ = calls[0]
            response.headers[_ETAG_HEADER] = await self._get_etag(key)
        return response

    async def _get_etag(self, key: str) -> str:
        data = await self._cache.get_raw(key)
        expire = await self._cache.get_expire(key)
        etag = blake2s(data).hexdigest()
        await self._cache.set(etag, True, expire=expire)
        return etag


class CacheDeleteMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
        if request.headers.get(_CLEAR_CACHE_HEADER) == _CLEAR_CACHE_HEADER_VALUE:
            with invalidate_further():
                return await call_next(request)
        return await call_next(request)
