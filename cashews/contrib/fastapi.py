from __future__ import annotations

import contextlib
from contextlib import nullcontext
from contextvars import ContextVar
from datetime import datetime, timezone
from hashlib import blake2s
from typing import Any, ContextManager, Sequence

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from cashews import Cache, Command, cache, invalidate_further
from cashews._typing import TTL
from cashews.picklers import DEFAULT_PICKLER
from cashews.ttl import ttl_to_seconds

_cache_max_age: ContextVar[int] = ContextVar("cache_control_max_age")

PREFIX = "fastapi:"
_CACHE_CONTROL_HEADER = "Cache-Control"
_AGE_HEADER = "Age"
_ETAG_HEADER = "ETag"
_IF_NOT_MATCH_HEADER = "If-None-Match"
_CLEAR_CACHE_HEADER = "Clear-Site-Data"

_NO_CACHE = "no-cache"  # disable GET
_NO_STORE = "no-store"  # disable GET AND SET
_MAX_AGE = "max-age="
_PRIVATE = "private"
_PUBLIC = "public"

_CLEAR_CACHE_HEADER_VALUE = "cache"
__all__ = [
    "cache_control_ttl",
    "CacheRequestControlMiddleware",
    "CacheEtagMiddleware",
    "CacheDeleteMiddleware",
]


def cache_control_ttl(default: TTL):
    def _ttl(*args, **kwargs):
        return _cache_max_age.get(ttl_to_seconds(default))

    return _ttl


class CacheRequestControlMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        cache_instance: Cache = cache,
        methods: Sequence[str] = ("get",),
        private=True,
        prefix_to_disable: str = "",
    ):
        self._private = private
        self._cache = cache_instance
        self._methods = methods
        self._prefix_to_disable = prefix_to_disable
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        context: ContextManager = nullcontext()
        cache_control_value = request.headers.get(_CACHE_CONTROL_HEADER)
        if request.method.lower() not in self._methods:
            return await call_next(request)
        to_disable = _to_disable(cache_control_value)
        if to_disable:
            context = self._cache.disabling(*to_disable, prefix=self._prefix_to_disable)
        with context, max_age(cache_control_value), self._cache.detect as detector:
            response = await call_next(request)
            calls = detector.calls_list
            if calls:
                key, _ = calls[0]
                expire = await self._cache.get_expire(key)
                if expire > 0:
                    response.headers[_CACHE_CONTROL_HEADER] = (
                        f"{_PRIVATE if self._private else _PUBLIC}, {_MAX_AGE}{expire}"
                    )
                    response.headers[_AGE_HEADER] = f"{expire}"
            else:
                response.headers[_CACHE_CONTROL_HEADER] = _NO_CACHE
            return response


@contextlib.contextmanager
def max_age(cache_control_value: str | None):
    if not cache_control_value:
        yield
        return
    _max_age = _get_max_age(cache_control_value)
    reset_token = None
    if _max_age:
        reset_token = _cache_max_age.set(_max_age)
    try:
        yield
    finally:
        if reset_token:
            _cache_max_age.reset(reset_token)


def _to_disable(cache_control_value: str | None) -> tuple[Command, ...]:
    if cache_control_value == _NO_CACHE:
        return (Command.GET,)
    if cache_control_value == _NO_STORE:
        return Command.GET, Command.SET
    if cache_control_value and _get_max_age(cache_control_value) == 0:
        return Command.GET, Command.SET
    return ()


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

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_etag = request.headers.get(_IF_NOT_MATCH_HEADER)
        if request_etag and await self._cache.exists(self._get_etag_key(request_etag)):
            return Response(status_code=304)

        used_key: None | str = None

        def set_callback(key: str, result: Any):
            if not key.endswith(":lock"):
                nonlocal used_key
                used_key = key

        with self._cache.detect as detector, self._cache.callback(set_callback, cmd=Command.SET):
            response = await call_next(request)
            calls = detector.calls_list
            if not calls and not used_key:
                return response
            if used_key:
                _key = used_key
                _data = None
            else:
                _key = calls[0][0]
                _data = calls[0][1][0]["value"]
            _etag = await self._set_etag(_key, _data)
            return self._response_etag(response, _etag, request_etag)

    def _response_etag(self, response: Response, etag: str | None, request_etag: str | None) -> Response:
        if etag is None:
            return response
        if etag == request_etag:
            return Response(status_code=304)
        if etag:
            response.headers[_ETAG_HEADER] = etag
        return response

    async def _set_etag(self, key: str, data: Any = None) -> str | None:
        _data = data or await self._cache.get(key)
        if _is_early_cache(_data):
            expire = (_data[0] - datetime.now(timezone.utc)).total_seconds()  # type: ignore[index]
            _data = _data[1]  # type: ignore[index]
        else:
            expire = await self._cache.get_expire(key)
        if expire < 0:
            return None
        etag = _get_etag(_data)
        await self._cache.set(self._get_etag_key(etag), True, expire=expire)
        return etag

    @staticmethod
    def _get_etag_key(etag: str) -> str:
        return f"{PREFIX}:etag:{etag}"


def _get_etag(cached_data: Any) -> str:
    if not isinstance(cached_data, bytes):
        cached_data = cached_data.body if isinstance(cached_data, Response) else DEFAULT_PICKLER.dumps(cached_data)
    return blake2s(cached_data).hexdigest()


def _is_early_cache(data: Any) -> bool:
    return isinstance(data, list) and isinstance(data[0], datetime)


class CacheDeleteMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
        if request.headers.get(_CLEAR_CACHE_HEADER) == _CLEAR_CACHE_HEADER_VALUE:
            with invalidate_further():
                return await call_next(request)
        return await call_next(request)
