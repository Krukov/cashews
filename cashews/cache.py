import inspect
from datetime import timedelta
from functools import wraps
from typing import Any, Dict, Optional, Type, Union
from urllib.parse import parse_qsl, urlparse

from .backends.interface import Backend, ProxyBackend
from .backends.memory import Memory
from .cache_utils import cache as cache_decor
from .cache_utils import early, fail, hit, locked, perf, rate_limit
from .key import get_call_values


def call_hook(command):
    def decor(func):
        @wraps(func)
        async def _func(*args, **kwargs):
            self = args[0]
            all_in_kwargs = get_call_values(func, args, kwargs, func_args=None)
            key = await self._hook_call(command, all_in_kwargs.get("key", ""))
            if "key" in all_in_kwargs:
                all_in_kwargs.pop("key")
                all_in_kwargs.pop("self")
                return await func(self, key, **all_in_kwargs)
            return await func(*args, **kwargs)

        return _func

    return decor


class Cache(ProxyBackend):
    def __init__(self):
        self.__init = False
        self.__address = None
        self._kwargs = {}
        self.execute_hooks = None
        super().__init__()

    def setup(self, settings_url: str, hooks=(), **kwargs):
        self.execute_hooks = hooks
        params = settings_url_parse(settings_url)
        params.update(kwargs)
        self._setup_backend(**params)

    def _setup_backend(self, backend: Type[Backend], **kwargs):
        self._target = backend(**kwargs)

    async def init(self, *args, **kwargs):
        self.setup(*args, **kwargs)
        await self._init()

    async def _init(self):
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

    rate_limit = rate_limit

    cache = cache_decor
    __call__ = cache_decor
    fail = fail
    early = early
    hit = hit
    perf = perf
    locked = locked


def _fix_params_types(params: Dict[str, str]) -> Dict[str, Union[str, int, bool, float]]:
    new_params = {}
    bool_keys = ("safe",)
    true_values = ("1", "true", "True")
    for key, value in params.items():
        if value.isdigit():
            value = int(value)
        elif key in bool_keys:
            value = value in true_values
        else:
            try:
                value = float(value)
            except ValueError:
                pass
        new_params[key] = value
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

    return params
