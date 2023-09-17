import contextlib
from contextlib import nullcontext
from contextvars import ContextVar
from hashlib import blake2s
from typing import List, Optional

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from cashews import Cache, Command, cache, invalidate_further

_CACHE_MAX_AGE = ContextVar("cache_control_max_age")

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


def cache_control_condition():
    pass


def cache_control_ttl():
    pass


class CacheRequestControlMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, cache_instance: Cache = cache, methods: List[str] = ("get",), private=True):
        self._private = private
        self._cache = cache_instance
        self._methods = methods
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        context = nullcontext()
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
                if expire:
                    response.headers[
                        _CACHE_CONTROL_HEADER
                    ] = f"{_PRIVATE if self._private else _PUBLIC}, {_MAX_AGE}{expire}"
                    response.headers[_AGE_HEADER] = f"{expire}"
            else:
                response.headers[_CACHE_CONTROL_HEADER] = _NO_CACHE
            return response

    @contextlib.contextmanager
    def max_age(self, cache_control_value: Optional[str]):
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

    @staticmethod
    def _to_disable(cache_control_value: Optional[str]) -> tuple[Command]:
        if cache_control_value == _NO_CACHE:
            return (Command.GET,)
        if cache_control_value == _NO_STORE:
            return Command.GET, Command.SET
        return tuple()

    @staticmethod
    def _get_max_age(cache_control_value: str) -> int:
        if not cache_control_value.startswith(_MAX_AGE):
            return 0
        try:
            return int(cache_control_value.replace(_MAX_AGE, ""))
        except (ValueError, TypeError):
            return 0


class CacheEtagMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, cache_instance: Cache = cache):
        self._cache = cache_instance
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
        etag = request.headers.get(_IF_NOT_MATCH_HEADER)
        if etag and await self._cache.exists(etag):
            return Response(status_code=304)

        with self._cache.detect as detector:
            response = await call_next(request)
            calls = detector.calls_list
            if not calls:
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
