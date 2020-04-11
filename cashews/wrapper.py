import asyncio
from datetime import timedelta
from functools import partial, wraps
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type, Union
from urllib.parse import parse_qsl, urlparse

from . import cache_utils
from .backends.interface import Backend, ProxyBackend
from .backends.memory import Memory, MemoryInterval
from .helpers import _auto_init, _is_disable_middleware
from .typing import TTL, FuncArgsType

#  pylint: disable=too-many-public-methods


class Cache(ProxyBackend):
    def __init__(self):
        self.__init = False
        self.__address = None
        self._kwargs = {}
        self._disable: Union[bool, List] = False
        self.middlewares = (_is_disable_middleware, _auto_init)
        super().__init__()

    @property
    def is_init(self):
        return self.__init

    def is_disable(self, *cmds: str) -> bool:
        if isinstance(self._disable, bool):
            return self._disable
        for cmd in cmds:
            if cmd.lower() in [c.lower() for c in self._disable]:
                return True
        return False

    def is_enable(self, *cmds):
        return not self.is_disable(*cmds)

    def disable(self, *cmds: str):
        if self._disable is True:
            self._disable = ["cmds", "decorators"]
        if self._disable is False:
            self._disable = []
        self._disable.extend(cmds)

    def enable(self, *cmds: str):
        if self._disable is True:
            self._disable = ["cmds", "decorators"]
        if self._disable is False:
            self._disable = []
            return
        for cmd in cmds:
            if cmd in self._disable:
                self._disable.remove(cmd)

    @property
    def disable_info(self):
        return self._disable

    def setup(self, settings_url: str, middlewares: Tuple = (), **kwargs):
        self.middlewares = tuple(middlewares) + self.middlewares
        params = settings_url_parse(settings_url)
        params.update(kwargs)
        if "disable" in params:
            self._disable = params.pop("disable")
        else:
            self._disable = not params.pop("enable", True)
        if not isinstance(self._disable, bool):
            self._disable = list(self._disable)
        self._setup_backend(**params)

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
        expire = expire() if expire and callable(expire) else expire
        expire = expire.total_seconds() if expire and isinstance(expire, timedelta) else expire
        return self._with_middlewares("set", self._target.set)(key=key, value=value, expire=expire, exist=exist)

    def get(self, key: str) -> Any:
        return self._with_middlewares("get", self._target.get)(key=key)

    def get_many(self, *keys: str):
        return self._with_middlewares("get_many", self._target.get_many)(*keys)

    def incr(self, key: str) -> int:
        return self._with_middlewares("incr", self._target.incr)(key=key)

    def delete(self, key: str):
        return self._with_middlewares("delete", self._target.delete)(key=key)

    def delete_match(self, pattern: str):
        return self._with_middlewares("delete_match", self._target.delete_match)(pattern=pattern)

    def expire(self, key: str, timeout: TTL):
        timeout = timeout() if callable(timeout) else timeout
        timeout = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
        return self._with_middlewares("expire", self._target.expire)(key=key, timeout=timeout)

    def get_expire(self, key: str) -> int:
        return self._with_middlewares("get_expire", self._target.get_expire)(key=key)

    def set_lock(self, key: str, value: Any, expire: TTL) -> bool:
        expire = expire() if callable(expire) else expire
        expire = expire.total_seconds() if isinstance(expire, timedelta) else expire
        return self._with_middlewares("lock", self._target.set_lock)(key=key, value=value, expire=expire)

    def unlock(self, key: str, value: str) -> bool:
        return self._with_middlewares("unlock", self._target.unlock)(key=key, value=value)

    def ping(self, message: Optional[str] = None) -> str:
        return self._with_middlewares("ping", self._target.ping)(message=message)

    def clear(self):
        return self._with_middlewares("clear", self._target.clear)()

    def is_locked(self, key: str, wait: Union[float, None, TTL] = None, step: Union[int, float] = 0.1) -> bool:
        wait = wait() if wait and callable(wait) else wait
        wait = wait.total_seconds() if wait and isinstance(wait, timedelta) else wait
        return self._with_middlewares("is_locked", self._target.is_locked)(key=key, wait=wait, step=step)

    def _wrap_on_enable(self, name, decorator):
        def _decorator(func):
            @wraps(func)
            async def _call(*args, **kwargs):
                if self.is_disable("decorators", name):
                    return await func(*args, **kwargs)
                result = await decorator(func)(*args, **kwargs)
                if getattr(func, "_key_template", None):
                    _call._key_template = func._key_template
                return result

            return _call

        return _decorator

    # DecoratorS
    def rate_limit(
        self,
        limit: int,
        period: TTL,
        ttl: Optional[TTL] = None,
        func_args: FuncArgsType = None,
        action: Optional[Callable] = None,
        prefix="rate_limit",
    ):  # pylint: disable=too-many-arguments
        return self._wrap_on_enable(
            "rate_limit",
            cache_utils.rate_limit(
                self, limit=limit, period=period, ttl=ttl, func_args=func_args, action=action, prefix=prefix
            ),
        )

    def __call__(
        self,
        ttl: TTL,
        func_args: FuncArgsType = None,
        key: Optional[str] = None,
        store: Optional[Callable[[Any], bool]] = None,
        prefix: str = "",
    ):
        return self._wrap_on_enable(
            prefix or "cache",
            cache_utils.cache(self, ttl=ttl, func_args=func_args, key=key, store=store, prefix=prefix),
        )

    cache = __call__

    def invalidate(self, target, args_map: Optional[Dict[str, str]] = None, defaults: Optional[Dict] = None):
        return self._wrap_on_enable(
            "cache", cache_utils.invalidate(self, target=target, args_map=args_map, defaults=defaults)
        )

    def fail(
        self,
        ttl: TTL,
        exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
        key: Optional[str] = None,
        func_args: FuncArgsType = None,
        prefix: str = "fail",
    ):
        return self._wrap_on_enable(
            prefix, cache_utils.fail(self, ttl=ttl, exceptions=exceptions, key=key, func_args=func_args, prefix=prefix)
        )

    def circuit_breaker(
        self,
        errors_rate: int,
        period: TTL,
        ttl: TTL,
        exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
        key: Optional[str] = None,
        func_args: FuncArgsType = None,
        prefix: str = "circuit_breaker",
    ):

        return self._wrap_on_enable(
            prefix,
            cache_utils.circuit_breaker(
                self,
                errors_rate=errors_rate,
                period=period,
                ttl=ttl,
                exceptions=exceptions,
                key=key,
                func_args=func_args,
                prefix=prefix,
            ),
        )

    def early(
        self,
        ttl: TTL,
        func_args: FuncArgsType = None,
        key: Optional[str] = None,
        store: Optional[Callable[[Any], bool]] = None,
        prefix: str = "early",
    ):
        return self._wrap_on_enable(
            prefix, cache_utils.early(self, ttl=ttl, func_args=func_args, key=key, store=store, prefix=prefix)
        )

    def hit(
        self,
        ttl: TTL,
        cache_hits: int,
        update_before: int = 0,
        func_args: FuncArgsType = None,
        key: Optional[str] = None,
        store: Optional[Callable[[Any], bool]] = None,
        prefix: str = "hit",
    ):
        return self._wrap_on_enable(
            prefix,
            cache_utils.hit(
                self,
                ttl=ttl,
                cache_hits=cache_hits,
                update_before=update_before,
                func_args=func_args,
                key=key,
                store=store,
                prefix=prefix,
            ),
        )

    def dynamic(
        self,
        func_args: FuncArgsType = None,
        key: Optional[str] = None,
        store: Optional[Callable[[Any], bool]] = None,
        prefix: str = "dynamic",
    ):
        return self._wrap_on_enable(
            prefix,
            cache_utils.hit(
                self,
                ttl=60 * 60 * 24,
                cache_hits=1,
                update_before=0,
                func_args=func_args,
                key=key,
                store=store,
                prefix=prefix,
            ),
        )

    def perf(
        self,
        ttl: TTL,
        func_args: FuncArgsType = None,
        key: Optional[str] = None,
        trace_size: int = 10,
        perf_condition: Optional[Callable[[float, Iterable[float]], bool]] = None,
        prefix: str = "perf",
    ):
        return self._wrap_on_enable(
            prefix,
            cache_utils.perf(
                self,
                ttl=ttl,
                func_args=func_args,
                key=key,
                trace_size=trace_size,
                perf_condition=perf_condition,
                prefix=prefix,
            ),
        )

    def locked(
        self,
        ttl: Optional[TTL] = None,
        func_args: FuncArgsType = None,
        key: Optional[str] = None,
        step: Union[int, float] = 0.1,
        prefix: str = "locked",
    ):
        return self._wrap_on_enable(
            prefix, cache_utils.locked(self, ttl=ttl, func_args=func_args, key=key, step=step, prefix=prefix)
        )


def _fix_params_types(params: Dict[str, str]) -> Dict[str, Union[str, int, bool, float]]:
    new_params = {}
    bool_keys = ("safe", "enable", "disable")
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
        from cashews.backends.redis import Redis

        params["backend"] = Redis
        params["address"] = parse_result._replace(query=None)._replace(fragment=None).geturl()
    elif parse_result.scheme == "mem":
        params["backend"] = Memory
        if params.get("check_interval"):
            params["backend"] = MemoryInterval
    return params
