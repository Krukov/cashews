import asyncio
from contextvars import ContextVar
from functools import partial, wraps
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type, Union
from urllib.parse import parse_qsl, urlparse

from . import decorators, validation
from .backends.client_side import BcastClientSide, UpdateChannelClientSide
from .backends.interface import Backend, ProxyBackend
from .backends.memory import Memory, MemoryInterval
from .backends.redis import Redis
from .helpers import _auto_init, _is_disable_middleware
from .key import ttl_to_seconds
from .typing import TTL, CacheCondition

#  pylint: disable=too-many-public-methods


class Cache(ProxyBackend):
    def __init__(self):
        self.__init = False
        self.__address = None
        self._kwargs = {}
        self.__disable = ContextVar(str(id(self)), default=[])
        self._set_disable(False)
        self.middlewares = (_is_disable_middleware, _auto_init, validation._invalidate_middleware)
        super().__init__()

    @property
    def _disable(self) -> List:
        return list(self.__disable.get())

    def _set_disable(self, value):
        if value is True:
            value = ["cmds", "decorators"]
        elif value is False:
            value = []
        self.__disable.set(value)

    @property
    def is_init(self):
        return self.__init

    def is_disable(self, *cmds: str) -> bool:
        _disable = self._disable
        if not cmds and _disable:
            return True
        for cmd in cmds:
            if cmd.lower() in [c.lower() for c in _disable]:
                return True
        return False

    def is_enable(self, *cmds):
        return not self.is_disable(*cmds)

    def disable(self, *cmds: str):
        _disable = self._disable
        if not cmds:
            _disable = ["cmds", "decorators"]
        if self._disable is False:
            _disable = []
        _disable.extend(cmds)
        self._set_disable(_disable)

    def enable(self, *cmds: str):
        _disable = self._disable
        if not cmds:
            _disable = []
        for cmd in cmds:
            if cmd in _disable:
                _disable.remove(cmd)
        self._set_disable(_disable)

    @property
    def disable_info(self):
        return self._disable

    def setup(self, settings_url: str, middlewares: Tuple = (), **kwargs):
        self.middlewares = tuple(middlewares) + self.middlewares
        params = settings_url_parse(settings_url)
        params.update(kwargs)
        if "disable" in params:
            self._set_disable(params.pop("disable"))
        else:
            self._set_disable(not params.pop("enable", True))
        if "client_side" in params:

            client_side = params.pop("client_side")
            params["backend"] = BcastClientSide
            if client_side == "update":
                params["backend"] = UpdateChannelClientSide

        self._setup_backend(**params)
        return self

    def _setup_backend(self, backend: Type[Backend], **kwargs):
        if self._target:
            asyncio.create_task(self._target.close())
        self._target = backend(**kwargs)
        self.__init = False

    async def init(self, *args, **kwargs):
        self.setup(*args, **kwargs)
        await self._init()

    async def _init(self):
        if self.is_disable():
            return None
        if not self.__init:
            await self._target.init()
        self.__init = True

    def _with_middlewares(self, cmd: str, target):
        call = target
        for middleware in self.middlewares:
            call = partial(middleware, call, cmd=cmd, backend=self)
        return call

    def set(self, key: str, value: Any, expire: Union[float, None, TTL] = None, exist: Optional[bool] = None):
        return self._with_middlewares("set", self._target.set)(
            key=key, value=value, expire=ttl_to_seconds(expire), exist=exist
        )

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self._with_middlewares("get", self._target.get)(key=key, default=default)

    def get_many(self, *keys: str):
        return self._with_middlewares("get_many", self._target.get_many)(*keys)

    def incr(self, key: str) -> int:
        return self._with_middlewares("incr", self._target.incr)(key=key)

    def delete(self, key: str):
        return self._with_middlewares("delete", self._target.delete)(key=key)

    def delete_match(self, pattern: str):
        return self._with_middlewares("delete_match", self._target.delete_match)(pattern=pattern)

    def expire(self, key: str, timeout: TTL):
        return self._with_middlewares("expire", self._target.expire)(key=key, timeout=ttl_to_seconds(timeout))

    def get_expire(self, key: str) -> int:
        return self._with_middlewares("get_expire", self._target.get_expire)(key=key)

    def set_lock(self, key: str, value: Any, expire: TTL) -> bool:
        return self._with_middlewares("lock", self._target.set_lock)(
            key=key, value=value, expire=ttl_to_seconds(expire)
        )

    def unlock(self, key: str, value: str) -> bool:
        return self._with_middlewares("unlock", self._target.unlock)(key=key, value=value)

    def listen(self, pattern: str, *cmds, reader=None):
        return self._with_middlewares("listen", self._target.listen)(pattern, *cmds, reader=reader)

    def ping(self, message: Optional[bytes] = None) -> str:
        return self._with_middlewares("ping", self._target.ping)(message=message)

    def clear(self):
        return self._with_middlewares("clear", self._target.clear)()

    def is_locked(self, key: str, wait: Union[float, None, TTL] = None, step: Union[int, float] = 0.1) -> bool:
        return self._with_middlewares("is_locked", self._target.is_locked)(
            key=key, wait=ttl_to_seconds(wait), step=step
        )

    def _wrap_on_enable(self, name, decorator):
        def _decorator(func):
            decorator(func)  # to register cache templates

            @wraps(func)
            async def _call(*args, **kwargs):
                if self.is_disable("decorators", name):
                    return await func(*args, **kwargs)
                return await decorator(func)(*args, **kwargs)

            return _call

        return _decorator

    def _wrap_on_enable_with_fail_disable(self, name, decorator):
        def _decorator(func):
            decorator(func)  # to register cache templates

            @wraps(func)
            async def _call(*args, **kwargs):
                if self.is_disable("decorators", name):
                    return await func(*args, **kwargs)
                detect = decorators.CacheDetect()
                result = await decorator(func)(*args, _from_cache=detect, **kwargs)
                if detect.get():
                    decorators.context_cache_detect.merge(detect)
                    self.disable("set")
                return result

            return _call

        return _decorator

    # DecoratorS
    def rate_limit(
        self,
        limit: int,
        period: TTL,
        ttl: Optional[TTL] = None,
        action: Optional[Callable] = None,
        prefix="rate_limit",
    ):  # pylint: disable=too-many-arguments
        return self._wrap_on_enable(
            "rate_limit",
            decorators.rate_limit(
                self, limit=limit, period=ttl_to_seconds(period), ttl=ttl_to_seconds(ttl), action=action, prefix=prefix,
            ),
        )

    def __call__(
        self, ttl: TTL, key: Optional[str] = None, condition: CacheCondition = None, prefix: str = "",
    ):
        return self._wrap_on_enable(
            prefix or "cache",
            decorators.cache(self, ttl=ttl_to_seconds(ttl), key=key, condition=condition, prefix=prefix),
        )

    cache = __call__

    def invalidate(self, target, args_map: Optional[Dict[str, str]] = None, defaults: Optional[Dict] = None):
        return self._wrap_on_enable(
            "cache", validation.invalidate(self, target=target, args_map=args_map, defaults=defaults)
        )

    invalidate_func = validation.invalidate_func

    def fail(
        self,
        ttl: TTL,
        exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        prefix: str = "fail",
    ):
        return self._wrap_on_enable_with_fail_disable(
            prefix,
            decorators.fail(
                self, ttl=ttl_to_seconds(ttl), exceptions=exceptions, key=key, condition=condition, prefix=prefix,
            ),
        )

    def circuit_breaker(
        self,
        errors_rate: int,
        period: TTL,
        ttl: TTL,
        exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
        key: Optional[str] = None,
        prefix: str = "circuit_breaker",
    ):

        return self._wrap_on_enable(
            prefix,
            decorators.circuit_breaker(
                self,
                errors_rate=errors_rate,
                period=ttl_to_seconds(period),
                ttl=ttl_to_seconds(ttl),
                exceptions=exceptions,
                key=key,
                prefix=prefix,
            ),
        )

    def early(
        self, ttl: TTL, key: Optional[str] = None, condition: CacheCondition = None, prefix: str = "early",
    ):
        return self._wrap_on_enable(
            prefix, decorators.early(self, ttl=ttl_to_seconds(ttl), key=key, condition=condition, prefix=prefix),
        )

    def hit(
        self,
        ttl: TTL,
        cache_hits: int,
        update_before: int = 0,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        prefix: str = "hit",
    ):
        return self._wrap_on_enable(
            prefix,
            decorators.hit(
                self,
                ttl=ttl_to_seconds(ttl),
                cache_hits=cache_hits,
                update_before=update_before,
                key=key,
                condition=condition,
                prefix=prefix,
            ),
        )

    def dynamic(
        self,
        ttl: TTL = 60 * 60 * 24,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        prefix: str = "dynamic",
    ):
        return self._wrap_on_enable(
            prefix,
            decorators.hit(
                self,
                ttl=ttl_to_seconds(ttl),
                cache_hits=3,
                update_before=2,
                key=key,
                condition=condition,
                prefix=prefix,
            ),
        )

    def perf(
        self,
        ttl: TTL,
        key: Optional[str] = None,
        trace_size: int = 10,
        perf_condition: Optional[Callable[[float, Iterable[float]], bool]] = None,
        prefix: str = "perf",
    ):
        return self._wrap_on_enable(
            prefix,
            decorators.perf(
                self,
                ttl=ttl_to_seconds(ttl),
                key=key,
                trace_size=trace_size,
                perf_condition=perf_condition,
                prefix=prefix,
            ),
        )

    def locked(
        self,
        ttl: Optional[TTL] = None,
        key: Optional[str] = None,
        step: Union[int, float] = 0.1,
        prefix: str = "locked",
    ):
        return self._wrap_on_enable(
            prefix, decorators.locked(self, ttl=ttl_to_seconds(ttl), key=key, step=step, prefix=prefix)
        )


def _fix_params_types(params: Dict[str, str]) -> Dict[str, Union[str, int, bool, float]]:
    new_params = {}
    bool_keys = ("safe", "enable", "disable", "client_side")
    true_values = (
        "1",
        "true",
    )
    for key, value in params.items():
        if key.lower() in bool_keys:
            value = value.lower() in true_values
        elif value.isdigit():
            value = int(value)
        else:
            try:
                value = float(value)
            except ValueError:
                pass
        new_params[key.lower()] = value
    return new_params


def settings_url_parse(url):
    params = {}
    parse_result = urlparse(url)
    params.update(dict(parse_qsl(parse_result.query)))
    params = _fix_params_types(params)
    if parse_result.scheme == "redis":

        params["backend"] = Redis
        params["address"] = parse_result._replace(query=None)._replace(fragment=None).geturl()
    elif parse_result.scheme == "mem":
        params["backend"] = Memory
        if params.get("check_interval"):
            params["backend"] = MemoryInterval
    return params
