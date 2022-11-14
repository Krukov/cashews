import asyncio
from contextlib import contextmanager
from functools import lru_cache, partial, wraps
from typing import Any, AsyncIterator, Callable, Dict, Iterable, List, Mapping, Optional, Tuple, Type, Union

from . import decorators, validation
from ._typing import TTL, AsyncCallable_T, AsyncCallableResult_T, CacheCondition, Middleware
from .backend_settings import settings_url_parse
from .backends.interface import Backend, _BackendInterface
from .cache_condition import create_time_condition, get_cache_condition
from .commands import Command
from .disable_control import _is_disable_middleware
from .exceptions import NotConfiguredError
from .ttl import ttl_to_seconds

try:
    from .backends.client_side import BcastClientSide
except ImportError:
    BcastClientSide = None  # type: ignore

try:
    from .backends.diskcache import DiskCache
except ImportError:
    DiskCache = None  # type: ignore


#  pylint: disable=too-many-public-methods


def _create_auto_init() -> Middleware:
    lock = asyncio.Lock()

    async def _auto_init(
        call: AsyncCallable_T, cmd: Command, backend: Backend, *args, **kwargs
    ) -> AsyncCallableResult_T:
        if backend.is_init:
            return await call(*args, **kwargs)
        async with lock:
            if not backend.is_init:
                await backend.init()

        return await call(*args, **kwargs)

    return _auto_init


class Cache(_BackendInterface):
    default_prefix = ""

    def __init__(self, name: str = ""):
        self._backends: Dict[str, Tuple[Backend, Tuple[Middleware, ...]]] = {}  # {key: (backend, middleware)}
        self._default_middlewares: Tuple[Middleware, ...] = (
            _is_disable_middleware,
            _create_auto_init(),
            validation._invalidate_middleware,
        )
        self.name = name
        self._default_fail_exceptions: Tuple[Type[Exception], ...] = (Exception,)

    detect = decorators.context_cache_detect

    def set_default_fail_exceptions(self, *exc: Type[Exception]) -> None:
        self._default_fail_exceptions = exc

    def disable(self, *cmds: Command, prefix: str = "") -> None:
        return self._get_backend(prefix).disable(*cmds)

    def enable(self, *cmds: Command, prefix: str = "") -> None:
        return self._get_backend(prefix).enable(*cmds)

    @contextmanager
    def disabling(self, *cmds: Command, prefix: str = ""):
        self.disable(*cmds, prefix=prefix)
        try:
            yield
        finally:
            self.enable(*cmds, prefix=prefix)

    def is_disable(self, *cmds: Command, prefix: str = ""):
        return self._get_backend(prefix).is_disable(*cmds)

    def is_enable(self, *cmds: Command, prefix: str = ""):
        return not self.is_disable(*cmds, prefix=prefix)

    @lru_cache(maxsize=1000)
    def _get_backend_and_config(self, key: str) -> Tuple[Backend, Tuple[Middleware, ...]]:
        for prefix in sorted(self._backends.keys(), reverse=True):
            if key.startswith(prefix):
                return self._backends[prefix]
        if self.default_prefix not in self._backends:
            raise NotConfiguredError("run `cache.setup(...)` before using cache")
        return self._backends[self.default_prefix]

    def _get_backend(self, key: str) -> Backend:
        backend, _ = self._get_backend_and_config(key)
        return backend

    def setup(self, settings_url: str, middlewares: Tuple = (), prefix: str = default_prefix, **kwargs) -> Backend:
        backend_class, params = settings_url_parse(settings_url)
        params.update(kwargs)

        if "disable" in params:
            disable = params.pop("disable")
        else:
            disable = not params.pop("enable", True)

        backend = backend_class(**params)
        if disable:
            backend.disable()
        self._add_backend(backend, middlewares, prefix)
        return backend

    def _add_backend(self, backend: Backend, middlewares=(), prefix: str = default_prefix):
        self._backends[prefix] = (
            backend,
            self._default_middlewares + middlewares,
        )

    async def init(self, *args, **kwargs):
        if args or kwargs:
            self.setup(*args, **kwargs)
        for backend, _ in self._backends.values():
            await backend.init()

    @property
    def is_init(self) -> bool:
        for backend, _ in self._backends.values():
            if not backend.is_init:
                return False
        return True

    def _with_middlewares(self, cmd: Command, key: str):
        backend, middlewares = self._get_backend_and_config(key)
        return self._with_middlewares_for_backend(cmd, backend, middlewares)

    def _with_middlewares_for_backend(self, cmd: Command, backend, middlewares):
        call = getattr(backend, cmd.value)
        for middleware in middlewares:
            call = partial(middleware, call, cmd, backend)
        return call

    async def set(
        self,
        key: str,
        value: Any,
        expire: Union[float, TTL, None] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        return await self._with_middlewares(Command.SET, key)(
            key=key, value=value, expire=ttl_to_seconds(expire), exist=exist
        )

    async def set_raw(self, key: str, value: Any, **kwargs):
        return await self._with_middlewares(Command.SET_RAW, key)(key=key, value=value, **kwargs)

    async def get(self, key: str, default: Optional[Any] = None) -> Any:
        return await self._with_middlewares(Command.GET, key)(key=key, default=default)

    async def get_raw(self, key: str) -> Any:
        return await self._with_middlewares(Command.GET_RAW, key)(key=key)

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:  # type: ignore
        backend, middlewares = self._get_backend_and_config(pattern)

        async def call(pattern, batch_size):
            return backend.scan(pattern, batch_size=batch_size)

        for middleware in middlewares:
            call = partial(middleware, call, Command.SCAN, backend)
        async for key in (await call(pattern=pattern, batch_size=batch_size)):
            yield key

    async def get_match(  # type: ignore
        self,
        pattern: str,
        batch_size: int = 100,
    ) -> AsyncIterator[Tuple[str, Any]]:
        backend, middlewares = self._get_backend_and_config(pattern)

        async def call(pattern, batch_size):
            return backend.get_match(pattern, batch_size=batch_size)

        for middleware in middlewares:
            call = partial(middleware, call, Command.GET_MATCH, backend)
        async for key, value in (await call(pattern=pattern, batch_size=batch_size)):
            yield key, value

    async def get_many(self, *keys: str, default: Optional[Any] = None) -> Tuple[Any, ...]:
        backends: Dict[Backend, List[str]] = {}
        for key in keys:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        result: Dict[str, Any] = {}
        for _keys in backends.values():
            _values = await self._with_middlewares(Command.GET_MANY, _keys[0])(*_keys, default=default)
            result.update(dict(zip(_keys, _values)))
        return tuple(result.get(key) for key in keys)

    async def set_many(self, pairs: Mapping[str, Any], expire: Union[float, TTL, None] = None):
        backends: Dict[Backend, List[str]] = {}
        for key in pairs:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        for backend, keys in backends.items():
            data = {key: pairs[key] for key in keys}
            await self._with_middlewares(Command.SET_MANY, keys[0])(pairs=data, expire=ttl_to_seconds(expire))

    async def get_bits(self, key: str, *indexes: int, size: int = 1) -> Tuple[int, ...]:
        return await self._with_middlewares(Command.GET_BITS, key)(key, *indexes, size=size)

    async def incr_bits(self, key: str, *indexes: int, size: int = 1, by: int = 1):
        return await self._with_middlewares(Command.INCR_BITS, key)(key, *indexes, size=size, by=by)

    async def incr(self, key: str) -> int:
        return await self._with_middlewares(Command.INCR, key)(key=key)

    async def delete(self, key: str):
        return await self._with_middlewares(Command.DELETE, key)(key=key)

    async def delete_many(self, *keys: str) -> None:
        backends: Dict[Backend, List[str]] = {}
        for key in keys:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        for _keys in backends.values():
            await self._with_middlewares(Command.DELETE_MANY, _keys[0])(*_keys)

    async def delete_match(self, pattern: str):
        return await self._with_middlewares(Command.DELETE_MATCH, pattern)(pattern=pattern)

    async def expire(self, key: str, timeout: TTL):
        return await self._with_middlewares(Command.EXPIRE, key)(key=key, timeout=ttl_to_seconds(timeout))

    async def get_expire(self, key: str):
        return await self._with_middlewares(Command.GET_EXPIRE, key)(key=key)

    async def exists(self, key: str):
        return await self._with_middlewares(Command.EXIST, key)(key=key)

    async def set_lock(self, key: str, value: Any, expire: TTL):
        return await self._with_middlewares(Command.SET_LOCK, key)(key=key, value=value, expire=ttl_to_seconds(expire))

    async def unlock(self, key: str, value: str):
        return await self._with_middlewares(Command.UNLOCK, key)(key=key, value=value)

    async def get_size(self, key: str):
        return await self._with_middlewares(Command.GET_SIZE, key)(key=key)

    async def ping(self, message: Optional[bytes] = None) -> bytes:
        message = b"PING" if message is None else message
        return await self._with_middlewares(Command.PING, message.decode())(message=message)

    async def clear(self):
        for backend, _ in self._backends.values():
            await self._with_middlewares_for_backend(Command.CLEAR, backend, self._default_middlewares)()

    async def close(self):
        for backend, _ in self._backends.values():
            await backend.close()

    async def is_locked(
        self,
        key: str,
        wait: Union[float, None, TTL] = None,
        step: Union[int, float] = 0.1,
    ) -> bool:
        return await self._with_middlewares(Command.IS_LOCKED, key)(key=key, wait=ttl_to_seconds(wait), step=step)

    def _wrap_on(self, decorator_fabric, upper, **decor_kwargs):
        if upper:
            return self._wrap_with_condition(decorator_fabric, **decor_kwargs)
        return self._wrap(decorator_fabric, **decor_kwargs)

    def _wrap(self, decorator_fabric, lock=False, time_condition=None, **decor_kwargs):
        def _decorator(func: AsyncCallable_T) -> AsyncCallable_T:
            if time_condition is not None:
                condition, _decor = create_time_condition(time_condition)
                func = _decor(func)
                decor_kwargs["condition"] = condition

            decorator = decorator_fabric(self, **decor_kwargs)(func)

            @wraps(func)
            async def _call(*args, **kwargs):
                if lock:
                    _locked = decorators.locked(self, key=decor_kwargs.get("key"), ttl=decor_kwargs["ttl"])
                    return await _locked(decorator)(*args, **kwargs)
                else:
                    return await decorator(*args, **kwargs)

            return _call

        return _decorator

    def _wrap_with_condition(self, decorator_fabric, condition, lock=False, time_condition=None, **decor_kwargs):
        def _decorator(func: AsyncCallable_T) -> AsyncCallable_T:
            _condition = condition
            if time_condition is not None:
                _condition, _decor = create_time_condition(time_condition)
                func = _decor(func)
            decorator_fabric(self, **decor_kwargs)(func)  # to register cache templates

            @wraps(func)
            async def _call(*args, **kwargs):
                with decorators.context_cache_detect as detect:

                    def new_condition(result, _args, _kwargs, key):
                        if detect.keys:
                            return False
                        return _condition(result, _args, _kwargs, key=key) if _condition else result is not None

                    decorator = decorator_fabric(self, **decor_kwargs, condition=new_condition)
                    if lock:
                        _locked = decorators.locked(self, key=decor_kwargs.get("key"), ttl=decor_kwargs["ttl"])
                        result = await _locked(decorator(func))(*args, **kwargs)
                    else:
                        result = await decorator(func)(*args, **kwargs)

                return result

            return _call

        return _decorator

    # DecoratorS
    def __call__(
        self,
        ttl: TTL,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "",
        upper: bool = False,
        lock: bool = False,
    ):
        return self._wrap_on(
            decorators.cache,
            upper,
            lock=lock,
            ttl=ttl,
            key=key,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
        )

    cache = __call__

    def failover(
        self,
        ttl: TTL,
        exceptions: Union[Type[Exception], Iterable[Type[Exception]], None] = None,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "fail",
    ):
        exceptions = exceptions or self._default_fail_exceptions
        return self._wrap_with_condition(
            decorators.failover,
            ttl=ttl,
            exceptions=exceptions,
            key=key,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
        )

    def early(
        self,
        ttl: TTL,
        key: Optional[str] = None,
        early_ttl: Optional[TTL] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "early",
        upper: bool = False,
    ):
        return self._wrap_on(
            decorators.early,
            upper,
            ttl=ttl,
            key=key,
            early_ttl=early_ttl,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
        )

    def soft(
        self,
        ttl: TTL,
        key: Optional[str] = None,
        soft_ttl: Optional[TTL] = None,
        exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "soft",
        upper: bool = False,
    ):
        return self._wrap_on(
            decorators.soft,
            upper,
            ttl=ttl,
            key=key,
            soft_ttl=ttl_to_seconds(soft_ttl),
            exceptions=exceptions,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
        )

    def hit(
        self,
        ttl: TTL,
        cache_hits: int,
        update_after: int = 0,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "hit",
        upper: bool = False,
    ):
        return self._wrap_on(
            decorators.hit,
            upper,
            ttl=ttl,
            cache_hits=cache_hits,
            update_after=update_after,
            key=key,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
        )

    def dynamic(
        self,
        ttl: TTL = 60 * 60 * 24,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "dynamic",
        upper: bool = False,
    ):
        return self._wrap_on(
            decorators.hit,
            upper,
            ttl=ttl,
            cache_hits=3,
            update_after=1,
            key=key,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
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
        half_open_ttl: TTL = None,
        exceptions: Union[Type[Exception], Tuple[Type[Exception], ...], None] = None,
        key: Optional[str] = None,
        prefix: str = "circuit_breaker",
    ):
        _exceptions = exceptions or self._default_fail_exceptions
        return decorators.circuit_breaker(
            backend=self,
            errors_rate=errors_rate,
            period=ttl_to_seconds(period),
            ttl=ttl_to_seconds(ttl),
            half_open_ttl=ttl_to_seconds(half_open_ttl),
            exceptions=_exceptions,
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
            period=period,
            ttl=ttl,
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
            ttl=ttl,
            key=key,
            step=step,
            prefix=prefix,
        )

    def bloom(
        self,
        *,
        capacity: int,
        name: Optional[str] = None,
        false_positives: Optional[Union[float, int]] = 1,
        check_false_positive: bool = True,
        prefix: str = "bloom",
    ):
        return decorators.bloom(
            backend=self,
            name=name,
            false_positives=false_positives,
            capacity=capacity,
            check_false_positive=check_false_positive,
            prefix=prefix,
        )

    def dual_bloom(
        self,
        *,
        capacity: int,
        name: Optional[str] = None,
        false: Optional[Union[float, int]] = 1,
        no_collisions: bool = False,
        prefix: str = "dual_bloom",
    ):
        return decorators.dual_bloom(
            backend=self,
            name=name,
            false=false,
            no_collisions=no_collisions,
            capacity=capacity,
            prefix=prefix,
        )
