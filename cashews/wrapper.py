import asyncio
import inspect
from datetime import timedelta
from functools import wraps
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Type, Union
from urllib.parse import parse_qsl, urlparse

from . import cache_utils
from .backends.interface import Backend, ProxyBackend
from .backends.memory import Memory, MemoryInterval
from .key import FuncArgsType, get_call_values


def call_hook(command):
    def decor(func):
        @wraps(func)
        async def _func(*args, **kwargs):
            self = args[0]
            if self.disable:
                return None
            all_in_kwargs = get_call_values(func, args, kwargs, func_args=None)
            key = await self._hook_call(command, all_in_kwargs.get("key", ""))
            if "key" in all_in_kwargs:
                all_in_kwargs.pop("key")
                all_in_kwargs.pop("self")
                return await func(self, key, **all_in_kwargs)
            return await func(*args, **kwargs)

        return _func

    return decor


def _not_decorator(func):
    return func


class Cache(ProxyBackend):
    def __init__(self):
        self.__init = False
        self.__address = None
        self._kwargs = {}
        self.enable = True
        self.execute_hooks = ()
        super().__init__()

    @property
    def disable(self):
        return not self.enable

    def setup(self, settings_url: str, hooks=(), **kwargs):
        self.execute_hooks = hooks
        params = settings_url_parse(settings_url)
        params.update(kwargs)
        if "disable" in params:
            self.enable = not params.pop("disable")
        else:
            self.enable = params.pop("enable", True)
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
        if self.disable:
            return None
        if not self.__init:
            await self._target.init()
        self.__init = True

    async def _hook_call(self, command, key):
        if not self.__init:
            await self._init()

        for hook in self.execute_hooks:
            call = hook(command, key)
            if inspect.isawaitable(call):
                call = await call
            if call is not None:
                key = call
        return key

    @call_hook("SET")
    async def set(
        self, key: str, value: Any, expire: Union[None, float, int, timedelta] = None, exist: Optional[bool] = None
    ) -> bool:
        expire = expire.total_seconds() if expire and isinstance(expire, timedelta) else expire
        return await self._target.set(key, value, expire=expire, exist=exist)

    @call_hook("GET")
    async def get(self, key: str) -> Any:
        return await self._target.get(key)

    @call_hook("INCR")
    async def incr(self, key: str) -> int:
        return await self._target.incr(key)

    @call_hook("DELETE")
    async def delete(self, key: str):
        return await self._target.delete(key)

    @call_hook("DELETE_MATCH")
    async def delete_match(self, pattern: str):
        return await self._target.delete_match(pattern)

    @call_hook("EXPIRE")
    async def expire(self, key: str, timeout: Union[float, int, timedelta]):
        timeout = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
        return await self._target.expire(key, timeout)

    @call_hook("LOCK")
    async def set_lock(self, key: str, value: Any, expire: Union[float, int, timedelta]) -> bool:
        expire = expire.total_seconds() if isinstance(expire, timedelta) else expire
        return await self._target.set_lock(key, value, expire=expire)

    @call_hook("UNLOCK")
    async def unlock(self, key: str, value: str) -> bool:
        return await self._target.unlock(key, value)

    @call_hook("PING")
    async def ping(self, message: Optional[str] = None) -> str:
        return await self._target.ping(message)

    @call_hook("CLEAR")
    async def clear(self):
        return await self._target.clear()

    async def is_locked(self, key: str, wait: Union[int, float, None] = None) -> bool:
        wait = wait.total_seconds() if wait and isinstance(wait, timedelta) else wait
        return await self._target.is_locked(key, wait=wait)

    # DecoratorS
    def rate_limit(
        self,
        limit: int,
        period: Union[int, timedelta],
        ttl: Optional[Union[int, timedelta]] = None,
        func_args: FuncArgsType = None,
        action: Optional[Callable] = None,
        prefix="rate_limit",
    ):  # pylint: disable=too-many-arguments
        if self.disable:
            return _not_decorator
        return cache_utils.rate_limit(
            self, limit=limit, period=period, ttl=ttl, func_args=func_args, action=action, prefix=prefix
        )

    def __call__(
        self,
        ttl: Union[int, timedelta],
        func_args: FuncArgsType = None,
        key: Optional[str] = None,
        disable: Optional[Callable[[FuncArgsType], bool]] = None,
        store: Optional[Callable[[Any], bool]] = None,
        prefix: str = "",
    ):
        if self.disable:
            return _not_decorator

        return cache_utils.cache(
            self, ttl=ttl, func_args=func_args, key=key, disable=disable, store=store, prefix=prefix
        )

    cache = __call__

    def invalidate(self, target, args_map: Optional[Dict[str, str]] = None, defaults: Optional[Dict] = None):
        return cache_utils.invalidate(self, target=target, args_map=args_map, defaults=defaults)

    def fail(
        self,
        ttl: Union[int, timedelta],
        exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
        key: Optional[str] = None,
        func_args: FuncArgsType = None,
        prefix: str = "fail",
    ):
        if self.disable:
            return _not_decorator

        return cache_utils.fail(self, ttl=ttl, exceptions=exceptions, key=key, func_args=func_args, prefix=prefix)

    def early(
        self,
        ttl: Optional[Union[int, timedelta]],
        func_args: FuncArgsType = None,
        key: Optional[str] = None,
        disable: Optional[Callable[[FuncArgsType], bool]] = None,
        store: Optional[Callable[[Any], bool]] = None,
        prefix: str = "early",
    ):
        if self.disable:
            return _not_decorator

        return cache_utils.early(
            self, ttl=ttl, func_args=func_args, key=key, disable=disable, store=store, prefix=prefix
        )

    def hit(
        self,
        ttl: Union[int, timedelta],
        cache_hits: int,
        update_before: int = 0,
        func_args: FuncArgsType = None,
        key: Optional[str] = None,
        disable: Optional[Callable[[FuncArgsType], bool]] = None,
        store: Optional[Callable[[Any], bool]] = None,
        prefix: str = "hit",
    ):
        if self.disable:
            return _not_decorator

        return cache_utils.hit(
            self,
            ttl=ttl,
            cache_hits=cache_hits,
            update_before=update_before,
            func_args=func_args,
            key=key,
            disable=disable,
            store=store,
            prefix=prefix,
        )

    def perf(
        self,
        ttl: Union[int, timedelta],
        func_args: FuncArgsType = None,
        key: Optional[str] = None,
        trace_size: int = 10,
        perf_condition: Optional[Callable[[float, Iterable[float]], bool]] = None,
        prefix: str = "perf",
    ):
        if self.disable:
            return _not_decorator

        return cache_utils.perf(
            self,
            ttl=ttl,
            func_args=func_args,
            key=key,
            trace_size=trace_size,
            perf_condition=perf_condition,
            prefix=prefix,
        )

    def locked(
        self,
        ttl: Union[int, timedelta],
        func_args: FuncArgsType = None,
        key: Optional[str] = None,
        lock_ttl: Union[int, timedelta] = 1,
        prefix: str = "lock",
    ):
        if self.disable:
            return _not_decorator

        return cache_utils.locked(self, ttl=ttl, func_args=func_args, key=key, lock_ttl=lock_ttl, prefix=prefix)


def _fix_params_types(params: Dict[str, str]) -> Dict[str, Union[str, int, bool, float]]:
    new_params = {}
    bool_keys = ("safe", "enable", "disable")
    true_values = (
        "1",
        "true",
    )
    for key, value in params.items():
        if value.isdigit():
            value = int(value)
        elif key.lower() in bool_keys:
            value = value.lower() in true_values
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
