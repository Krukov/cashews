import asyncio
from contextlib import contextmanager
from functools import partial, wraps
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Type, Union
from urllib.parse import parse_qsl, urlparse

from . import decorators, validation
from .backends.interface import Backend
from .backends.memory import Memory
from .disable_control import ControlMixin, _is_disable_middleware
from .key import ttl_to_seconds
from .typing import TTL, CacheCondition

try:
    import aioredis
except ImportError:
    BcastClientSide, IndexRedis, Redis = None, None, None
else:
    from .backends.client_side import BcastClientSide
    from .backends.index import IndexRedis
    from .backends.redis import Redis

    del aioredis

try:
    import diskcache
except ImportError:
    DiskCache = None
else:
    from .backends.diskcache import DiskCache


#  pylint: disable=too-many-public-methods


class BackendNotAvailable(Exception):
    pass


def _create_auto_init():
    lock = asyncio.Lock()

    async def _auto_init(call, *args, backend=None, cmd=None, **kwargs):
        if backend.is_init:
            return await call(*args, **kwargs)
        async with lock:
            if not backend.is_init:
                await backend.init()

        return await call(*args, **kwargs)

    return _auto_init


class Cache(Backend):
    default_prefix = ""

    def __init__(self, name=None):
        self._backends = {}  # {key: (backend, middleware)}
        self._default_middlewares = (
            _is_disable_middleware,
            _create_auto_init(),
            validation._invalidate_middleware,
        )
        self._name = name
        self._default_fail_exceptions = Exception
        self._add_backend(Memory)

    detect = decorators.context_cache_detect

    def set_default_fail_exceptions(self, exc: Union[Type[Exception], Iterable[Type[Exception]]]):
        self._default_fail_exceptions = exc

    def disable(self, *cmds, prefix=""):
        return self._get_backend(prefix).disable(*cmds)

    def disable_all(self, *cmds):
        for backend, _ in self._backends.values():
            backend.disable(*cmds)

    def enable(self, *cmds, prefix=""):
        return self._get_backend(prefix).enable(*cmds)

    def enable_all(self, *cmds):
        for backend, _ in self._backends.values():
            backend.enable(*cmds)

    @contextmanager
    def disabling(self, *cmds, prefix=""):
        self.disable(*cmds, prefix=prefix)
        yield
        self.enable(*cmds, prefix=prefix)

    def is_disable(self, *cmds, prefix=""):
        return self._get_backend(prefix).is_disable(*cmds)

    def is_enable(self, *cmds, prefix=""):
        return not self.is_disable(*cmds, prefix=prefix)

    def _get_backend_and_config(self, key) -> Tuple[Backend, Tuple[Callable]]:
        for prefix in sorted(self._backends.keys(), reverse=True):
            if key.startswith(prefix):
                return self._backends[prefix]
        return self._backends[self.default_prefix]

    def _get_backend(self, key) -> Backend:
        backend, _ = self._get_backend_and_config(key)
        return backend

    def setup(self, settings_url: str, middlewares: Tuple = (), prefix=default_prefix, **kwargs):
        params = settings_url_parse(settings_url)
        params.update(kwargs)

        if params.pop("client_side", None):
            params["backend"] = BcastClientSide
        if "index_name" in params:
            params["backend"] = IndexRedis
        backend = params.pop("backend")

        self._add_backend(backend, middlewares, prefix, **params)
        return self._backends[prefix][0]

    def _add_backend(self, backend_class, middlewares=(), prefix=default_prefix, **params):
        class _backend_class(ControlMixin, backend_class):
            pass

        self._backends[prefix] = (
            _backend_class(**params),
            self._default_middlewares + middlewares,
        )

    async def init(self, *args, **kwargs):
        if args or kwargs:
            self.setup(*args, **kwargs)
        for backend, _ in self._backends.values():
            await backend.init()

    def _with_middlewares(self, cmd: str, key):
        backend, middlewares = self._get_backend_and_config(key)
        return self._with_middlewares_for_backend(cmd, backend, middlewares)

    def _with_middlewares_for_backend(self, cmd: str, backend, middlewares):
        call = getattr(backend, cmd)
        for middleware in middlewares:
            call = partial(middleware, call, cmd=cmd, backend=backend)
        return call

    def set(
        self,
        key: str,
        value: Any,
        expire: Union[float, None, TTL] = None,
        exist: Optional[bool] = None,
    ):
        return self._with_middlewares("set", key)(key=key, value=value, expire=ttl_to_seconds(expire), exist=exist)

    def set_row(self, key: str, value: Any, **kwargs):
        return self._with_middlewares("set_row", key)(key=key, value=value, **kwargs)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self._with_middlewares("get", key)(key=key, default=default)

    def get_row(self, key: str) -> Any:
        return self._with_middlewares("get_row", key)(key=key)

    def keys_match(self, pattern: str):
        return self._with_middlewares("keys_match", pattern)(pattern=pattern)

    def get_many(self, *keys: str):
        return self._with_middlewares("get_many", keys[0])(*keys)

    def incr(self, key: str) -> int:
        return self._with_middlewares("incr", key)(key=key)

    def delete(self, key: str):
        return self._with_middlewares("delete", key)(key=key)

    def delete_match(self, pattern: str):
        return self._with_middlewares("delete_match", pattern)(pattern=pattern)

    def expire(self, key: str, timeout: TTL):
        return self._with_middlewares("expire", key)(key=key, timeout=ttl_to_seconds(timeout))

    def get_expire(self, key: str):
        return self._with_middlewares("get_expire", key)(key=key)

    def exists(self, key: str):
        return self._with_middlewares("exists", key)(key=key)

    def set_lock(self, key: str, value: Any, expire: TTL):
        return self._with_middlewares("set_lock", key)(key=key, value=value, expire=ttl_to_seconds(expire))

    def unlock(self, key: str, value: str):
        return self._with_middlewares("unlock", key)(key=key, value=value)

    def listen(self, pattern: str, *cmds, reader=None):
        return self._with_middlewares("listen", pattern)(pattern, *cmds, reader=reader)

    def ping(self, message: Optional[bytes] = None) -> str:
        message = b"PING" if message is None else message
        return self._with_middlewares("ping", message.decode())(message=message)

    async def clear(self):
        for backend, _ in self._backends.values():
            await self._with_middlewares_for_backend("clear", backend, self._default_middlewares)()

    def close(self):
        for backend, _ in self._backends.values():
            backend.close()

    def is_locked(
        self,
        key: str,
        wait: Union[float, None, TTL] = None,
        step: Union[int, float] = 0.1,
    ) -> bool:
        return self._with_middlewares("is_locked", key)(key=key, wait=ttl_to_seconds(wait), step=step)

    def _wrap_on(self, decorator_fabric, upper, **decor_kwargs):
        wrapper = self._wrap_on_enable
        if upper:
            wrapper = self._wrap_on_enable_with_condition
        return wrapper(decorator_fabric, **decor_kwargs)

    def _wrap_on_enable(self, decorator_fabric, **decor_kwargs):
        def _decorator(func):
            result_func = decorator_fabric(self, **decor_kwargs)(func)
            result_func.direct = func
            return result_func

        return _decorator

    def _wrap_on_enable_with_condition(self, decorator_fabric, condition, **decor_kwargs):
        def _decorator(func):
            decorator_fabric(self, **decor_kwargs)(func)  # to register cache templates

            @wraps(func)
            async def _call(*args, **kwargs):
                with decorators.context_cache_detect as detect:

                    def new_condition(result, _args, _kwargs, key):
                        if detect.keys:
                            return False
                        return condition(result, _args, _kwargs, key=key) if condition else result is not None

                    decorator = decorator_fabric(self, **decor_kwargs, condition=new_condition)
                    result = await decorator(func)(*args, **kwargs)

                return result

            _call.direct = func
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
            decorators.rate_limit,
            limit=limit,
            period=ttl_to_seconds(period),
            ttl=ttl_to_seconds(ttl),
            action=action,
            prefix=prefix,
        )

    def __call__(
        self,
        ttl: TTL,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        prefix: str = "",
        upper: bool = False,
    ):
        return self._wrap_on(
            decorators.cache,
            upper,
            ttl=ttl_to_seconds(ttl),
            key=key,
            condition=condition,
            prefix=prefix,
        )

    cache = __call__

    def invalidate(
        self,
        func,
        args_map: Optional[Dict[str, str]] = None,
        defaults: Optional[Dict] = None,
    ):
        return self._wrap_on_enable(
            validation.invalidate,
            target=func,
            args_map=args_map,
            defaults=defaults,
        )

    invalidate_func = validation.invalidate_func

    def failover(
        self,
        ttl: TTL,
        exceptions: Union[Type[Exception], Iterable[Type[Exception]], None] = None,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        prefix: str = "fail",
    ):
        exceptions = exceptions or self._default_fail_exceptions
        return self._wrap_on_enable_with_condition(
            decorators.failover,
            ttl=ttl_to_seconds(ttl),
            exceptions=exceptions,
            key=key,
            condition=condition,
            prefix=prefix,
        )

    def circuit_breaker(
        self,
        errors_rate: int,
        period: TTL,
        ttl: TTL,
        exceptions: Union[Type[Exception], Tuple[Type[Exception]], None] = None,
        key: Optional[str] = None,
        prefix: str = "circuit_breaker",
    ):
        exceptions = exceptions or self._default_fail_exceptions
        return self._wrap_on_enable(
            decorators.circuit_breaker,
            errors_rate=errors_rate,
            period=ttl_to_seconds(period),
            ttl=ttl_to_seconds(ttl),
            exceptions=exceptions,
            key=key,
            prefix=prefix,
        )

    def early(
        self,
        ttl: TTL,
        key: Optional[str] = None,
        early_ttl: Optional[TTL] = None,
        condition: CacheCondition = None,
        prefix: str = "early",
        upper: bool = False,
    ):
        return self._wrap_on(
            decorators.early,
            upper,
            ttl=ttl_to_seconds(ttl),
            key=key,
            early_ttl=ttl_to_seconds(early_ttl),
            condition=condition,
            prefix=prefix,
        )

    def hit(
        self,
        ttl: TTL,
        cache_hits: int,
        update_after: int = 0,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        prefix: str = "hit",
        upper: bool = False,
    ):
        return self._wrap_on(
            decorators.hit,
            upper,
            ttl=ttl_to_seconds(ttl),
            cache_hits=cache_hits,
            update_after=update_after,
            key=key,
            condition=condition,
            prefix=prefix,
        )

    def dynamic(
        self,
        ttl: TTL = 60 * 60 * 24,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        prefix: str = "dynamic",
        upper: bool = False,
    ):
        return self._wrap_on(
            decorators.hit,
            upper,
            ttl=ttl_to_seconds(ttl),
            cache_hits=3,
            update_after=1,
            key=key,
            condition=condition,
            prefix=prefix,
        )

    def locked(
        self,
        ttl: Optional[TTL] = None,
        key: Optional[str] = None,
        step: Union[int, float] = 0.1,
        prefix: str = "locked",
    ):
        return self._wrap_on_enable(
            decorators.locked,
            ttl=ttl_to_seconds(ttl),
            key=key,
            step=step,
            prefix=prefix,
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
        if Redis is None:
            raise BackendNotAvailable("Redis backend requires `aioredis` to be installed.")
        params["backend"] = Redis
        params["address"] = parse_result._replace(query=None)._replace(fragment=None).geturl()
    elif parse_result.scheme == "mem":
        params["backend"] = Memory
    elif parse_result.scheme == "disk":
        if DiskCache is None:
            raise BackendNotAvailable("Disk backend requires `diskcache` to be installed.")
        params["backend"] = DiskCache
    elif parse_result.scheme == "":
        params["backend"] = Memory
        params["disable"] = True
    return params
