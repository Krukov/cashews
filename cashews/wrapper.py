import asyncio
from contextlib import contextmanager
from functools import partial, wraps
from typing import Any, AsyncIterator, Callable, Dict, Iterable, Optional, Tuple, Type, Union
from urllib.parse import parse_qsl, urlparse

from . import decorators, validation
from ._typing import TTL, CacheCondition
from .backends.interface import Backend
from .backends.memory import Memory
from .disable_control import ControlMixin, _is_disable_middleware
from .key import ttl_to_seconds

try:
    try:
        from redis import asyncio as aioredis
    except ImportError:
        import aioredis
except ImportError:
    BcastClientSide, Redis = None, None
else:
    from .backends.client_side import BcastClientSide
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

    def __init__(self, name: Optional[str] = None):
        self._backends: Dict[str, Tuple[Backend, Tuple[Callable]]] = {}  # {key: (backend, middleware)}
        self._default_middlewares = (
            _is_disable_middleware,
            _create_auto_init(),
            validation._invalidate_middleware,
        )
        self._name = name
        self._default_fail_exceptions = Exception
        self._add_backend(Memory)

    detect = decorators.context_cache_detect

    def set_default_fail_exceptions(self, *exc: Type[Exception]):
        self._default_fail_exceptions = exc

    def disable(self, *cmds: str, prefix: str = ""):
        return self._get_backend(prefix).disable(*cmds)

    def disable_all(self, *cmds: str):
        for backend, _ in self._backends.values():
            backend.disable(*cmds)

    def enable(self, *cmds: str, prefix: str = ""):
        return self._get_backend(prefix).enable(*cmds)

    def enable_all(self, *cmds: str):
        for backend, _ in self._backends.values():
            backend.enable(*cmds)

    @contextmanager
    def disabling(self, *cmds: str, prefix: str = ""):
        self.disable(*cmds, prefix=prefix)
        yield
        self.enable(*cmds, prefix=prefix)

    def is_disable(self, *cmds: str, prefix: str = ""):
        return self._get_backend(prefix).is_disable(*cmds)

    def is_enable(self, *cmds: str, prefix: str = ""):
        return not self.is_disable(*cmds, prefix=prefix)

    def _get_backend_and_config(self, key: str) -> Tuple[Backend, Tuple[Callable]]:
        for prefix in sorted(self._backends.keys(), reverse=True):
            if key.startswith(prefix):
                return self._backends[prefix]
        return self._backends[self.default_prefix]

    def _get_backend(self, key: str) -> Backend:
        backend, _ = self._get_backend_and_config(key)
        return backend

    def setup(self, settings_url: str, middlewares: Tuple = (), prefix: str = default_prefix, **kwargs):
        params = settings_url_parse(settings_url)
        params.update(kwargs)

        if params.pop("client_side", None):
            params["backend"] = BcastClientSide
        backend = params.pop("backend")

        self._add_backend(backend, middlewares, prefix, **params)
        return self._backends[prefix][0]

    def _add_backend(self, backend_class: Type[Backend], middlewares=(), prefix: str = default_prefix, **params):
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
        expire: Union[float, TTL, None] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        return self._with_middlewares("set", key)(key=key, value=value, expire=ttl_to_seconds(expire), exist=exist)

    def set_raw(self, key: str, value: Any, **kwargs):
        return self._with_middlewares("set_raw", key)(key=key, value=value, **kwargs)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self._with_middlewares("get", key)(key=key, default=default)

    def get_raw(self, key: str) -> Any:
        return self._with_middlewares("get_raw", key)(key=key)

    async def keys_match(self, pattern: str):
        backend, middlewares = self._get_backend_and_config(pattern)

        async def call(_pattern):
            return backend.keys_match(_pattern)

        for middleware in middlewares:
            call = partial(middleware, call, cmd="keys_match", backend=backend)
        async for key in (await call(pattern)):
            yield key

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:
        backend, middlewares = self._get_backend_and_config(pattern)

        async def call(_pattern):
            return backend.keys_match(_pattern)

        for middleware in middlewares:
            call = partial(middleware, call, cmd="scan", backend=backend)
        async for key in (await call(pattern)):
            yield key

    async def get_match(
        self, pattern: str, batch_size: int = 100, default: Optional[Any] = None
    ) -> AsyncIterator[Tuple[str, Any]]:
        backend, middlewares = self._get_backend_and_config(pattern)

        async def call(_pattern, _batch_size, _default):
            return backend.get_match(_pattern, batch_size=_batch_size, default=_default)

        for middleware in middlewares:
            call = partial(middleware, call, cmd="get_match", backend=backend)
        async for key, value in (await call(pattern, batch_size, default)):
            yield key, value

    async def get_many(self, *keys: str, default: Optional[Any] = None) -> Tuple[Any]:
        backends = {}
        for key in keys:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        result = {}
        for _keys in backends.values():
            _values = await self._with_middlewares("get_many", _keys[0])(*_keys, default=default)
            result.update(dict(zip(_keys, _values)))
        return tuple([result.get(key) for key in keys])

    def get_bits(self, key: str, *indexes: int, size: int = 1) -> Tuple[int]:
        return self._with_middlewares("get_bits", key)(key, *indexes, size=size)

    def incr_bits(self, key: str, *indexes: int, size: int = 1, by: int = 1):
        return self._with_middlewares("incr_bits", key)(key, *indexes, size=size, by=by)

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
        if upper:
            return self._wrap_with_condition(decorator_fabric, **decor_kwargs)
        return self._wrap(decorator_fabric, **decor_kwargs)

    def _wrap(self, decorator_fabric, lock=False, **decor_kwargs):
        def _decorator(func):
            decorator = decorator_fabric(self, **decor_kwargs)(func)

            @wraps(func)
            async def _call(*args, **kwargs):
                if lock:
                    _locked = decorators.locked(self, key=decor_kwargs.get("key"), ttl=decor_kwargs["ttl"])
                    return await _locked(decorator)(*args, **kwargs)
                else:
                    return await decorator(*args, **kwargs)

            _call.direct = func
            return _call

        return _decorator

    def _wrap_with_condition(self, decorator_fabric, condition, lock=False, **decor_kwargs):
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
                    if lock:
                        _locked = decorators.locked(self, key=decor_kwargs.get("key"), ttl=decor_kwargs["ttl"])
                        result = await _locked(decorator(func))(*args, **kwargs)
                    else:
                        result = await decorator(func)(*args, **kwargs)

                return result

            _call.direct = func
            return _call

        return _decorator

    # DecoratorS
    def __call__(
        self,
        ttl: TTL,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        prefix: str = "",
        upper: bool = False,
        lock: bool = False,
    ):
        return self._wrap_on(
            decorators.cache,
            upper,
            lock=lock,
            ttl=ttl_to_seconds(ttl),
            key=key,
            condition=condition,
            prefix=prefix,
        )

    cache = __call__

    def failover(
        self,
        ttl: TTL,
        exceptions: Union[Type[Exception], Iterable[Type[Exception]], None] = None,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        prefix: str = "fail",
    ):
        exceptions = exceptions or self._default_fail_exceptions
        return self._wrap_with_condition(
            decorators.failover,
            ttl=ttl_to_seconds(ttl),
            exceptions=exceptions,
            key=key,
            condition=condition,
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

    def soft(
        self,
        ttl: TTL,
        key: Optional[str] = None,
        soft_ttl: Optional[TTL] = None,
        exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
        condition: CacheCondition = None,
        prefix: str = "soft",
        upper: bool = False,
    ):
        return self._wrap_on(
            decorators.soft,
            upper,
            ttl=ttl_to_seconds(ttl),
            key=key,
            soft_ttl=ttl_to_seconds(soft_ttl),
            exceptions=exceptions,
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

    def invalidate(
        self,
        func,
        args_map: Optional[Dict[str, str]] = None,
        defaults: Optional[Dict] = None,
    ):
        return validation.invalidate(
            backend=self,
            target=func,
            args_map=args_map,
            defaults=defaults,
        )

    invalidate_func = validation.invalidate_func

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
        return decorators.circuit_breaker(
            backend=self,
            errors_rate=errors_rate,
            period=ttl_to_seconds(period),
            ttl=ttl_to_seconds(ttl),
            exceptions=exceptions,
            key=key,
            prefix=prefix,
        )

    def rate_limit(
        self,
        limit: int,
        period: TTL,
        ttl: Optional[TTL] = None,
        action: Optional[Callable] = None,
        prefix="rate_limit",
    ):  # pylint: disable=too-many-arguments
        return decorators.rate_limit(
            backend=self,
            limit=limit,
            period=ttl_to_seconds(period),
            ttl=ttl_to_seconds(ttl),
            action=action,
            prefix=prefix,
        )

    def locked(
        self,
        ttl: Optional[TTL] = None,
        key: Optional[str] = None,
        step: Union[int, float] = 0.1,
        prefix: str = "locked",
    ):
        return decorators.locked(
            backend=self,
            ttl=ttl_to_seconds(ttl),
            key=key,
            step=step,
            prefix=prefix,
        )

    def bloom(
        self,
        name: Optional[str] = None,
        index_size: Optional[int] = None,
        number_of_hashes: Optional[int] = None,
        false_positives: Optional[Union[float, int]] = 1,
        capacity: Optional[int] = None,
        check_false_positive: bool = True,
        prefix: str = "bloom",
    ):
        return decorators.bloom(
            backend=self,
            name=name,
            index_size=index_size,
            number_of_hashes=number_of_hashes,
            false_positives=false_positives,
            capacity=capacity,
            check_false_positive=check_false_positive,
            prefix=prefix,
        )

    def dual_bloom(
        self,
        name: Optional[str] = None,
        index_size: Optional[int] = None,
        number_of_hashes: Optional[int] = None,
        false: Optional[Union[float, int]] = 1,
        no_collisions: bool = False,
        capacity: Optional[int] = None,
        prefix: str = "dual_bloom",
    ):
        return decorators.dual_bloom(
            backend=self,
            name=name,
            index_size=index_size,
            number_of_hashes=number_of_hashes,
            false=false,
            no_collisions=no_collisions,
            capacity=capacity,
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
    if parse_result.scheme == "redis" or parse_result.scheme == "rediss":
        if Redis is None:
            raise BackendNotAvailable("Redis backend requires `redis` (or `aioredis`) to be installed.")
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
    else:
        raise BackendNotAvailable(f"wrong backend alias {parse_result.scheme}")
    return params
