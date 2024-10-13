from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from cashews import validation
from cashews.backends.interface import Backend
from cashews.commands import Command
from cashews.exceptions import NotConfiguredError
from cashews.serialize import DEFAULT_SERIALIZER, Serializer, get_serializer

from ..picklers import PicklerType
from .auto_init import create_auto_init
from .backend_settings import settings_url_parse

if TYPE_CHECKING:  # pragma: no cover
    from cashews._typing import Key, Middleware


class Wrapper:
    default_prefix = ""

    def __init__(self, name: str = ""):
        self._backends: dict[str, Backend] = {}
        self._middlewares: dict[str, tuple[Middleware, ...]] = {}
        self._serializers: dict[str, Serializer] = {}
        self._sorted_prefixes: tuple[str, ...] = ()
        self._default_middlewares: list[Middleware] = [
            create_auto_init(),
            validation._invalidate_middleware,
        ]
        self.name = name
        super().__init__()

    def add_middleware(self, middleware: Middleware) -> None:
        self._default_middlewares.append(middleware)

    def _get_backend(self, key: Key) -> Backend:
        for prefix in self._sorted_prefixes:
            if key.startswith(prefix):
                return self._backends[prefix]
        self._check_setup()
        raise NotConfiguredError("Backend for given key not configured")

    def _call_with_middlewares_for_backend(self, *, call, cmd: Command, backend: Backend):
        for middleware in self._middlewares[backend._id]:
            call = partial(middleware, call, cmd, backend)
        return call

    def setup(
        self,
        settings_url: str,
        middlewares: tuple = (),
        prefix: str = default_prefix,
        **kwargs,
    ) -> Backend:
        backend_class, params, pickle_type = settings_url_parse(settings_url)
        params.update(kwargs)

        disable = params.pop("disable") if "disable" in params else not params.pop("enable", True)

        serializer = get_serializer(
            secret=params.pop("secret", None),
            digestmod=params.pop("digestmod", b"md5"),
            check_repr=params.pop("check_repr", True),
            pickle_type=PicklerType(params.pop("pickle_type", pickle_type)),
        )
        backend = backend_class(**params)
        if disable:
            backend.disable()
        self._add_backend(backend, middlewares, serializer, prefix)
        return backend

    def is_setup(self) -> bool:
        return bool(self._backends)

    def _check_setup(self) -> None:
        if not self._backends:
            raise NotConfiguredError("run `cache.setup(...)` before using cache")

    def _add_backend(
        self, backend: Backend, middlewares=(), serializer: Serializer | None = None, prefix: str = default_prefix
    ) -> None:
        serializer = serializer or DEFAULT_SERIALIZER
        self._backends[prefix] = backend
        self._middlewares[backend._id] = middlewares + tuple(self._default_middlewares)
        self._serializers[backend._id] = serializer
        self._sorted_prefixes = tuple(sorted(self._backends.keys(), reverse=True))

    async def init(self, *args, **kwargs) -> None:
        if args or kwargs:
            self.setup(*args, **kwargs)
        for backend in self._backends.values():
            await backend.init()

    @property
    def is_init(self) -> bool:
        return all(backend.is_init for backend in self._backends.values())

    async def close(self) -> None:
        for backend in self._backends.values():
            await backend.close()
