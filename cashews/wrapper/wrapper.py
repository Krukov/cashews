from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from cashews import validation
from cashews.backends.interface import Backend
from cashews.commands import Command
from cashews.exceptions import NotConfiguredError

from .auto_init import create_auto_init
from .backend_settings import settings_url_parse

if TYPE_CHECKING:  # pragma: no cover
    from cashews._typing import Key, Middleware


class Wrapper:
    default_prefix = ""

    def __init__(self, name: str = ""):
        self._backends: dict[str, tuple[Backend, tuple[Middleware, ...]]] = {}
        self._sorted_prefixes: tuple[str, ...] = ()
        self._default_middlewares: list[Middleware] = [
            create_auto_init(),
            validation._invalidate_middleware,
        ]
        self.name = name
        super().__init__()

    def add_middleware(self, middleware: Middleware) -> None:
        self._default_middlewares.append(middleware)

    def _get_backend_and_config(self, key: Key) -> tuple[Backend, tuple[Middleware, ...]]:
        for prefix in self._sorted_prefixes:
            if key.startswith(prefix):
                return self._backends[prefix]
        self._check_setup()
        raise NotConfiguredError("Backend for given key not configured")

    def _get_backend(self, key: Key) -> Backend:
        backend, _ = self._get_backend_and_config(key)
        return backend

    def _with_middlewares(self, cmd: Command, key: Key):
        backend, middlewares = self._get_backend_and_config(key)
        return self._with_middlewares_for_backend(cmd, backend, middlewares)

    def _with_middlewares_for_backend(self, cmd: Command, backend, middlewares):
        call = getattr(backend, cmd.value)
        for middleware in middlewares:
            call = partial(middleware, call, cmd, backend)
        return call

    def setup(
        self,
        settings_url: str,
        middlewares: tuple = (),
        prefix: str = default_prefix,
        **kwargs,
    ) -> Backend:
        backend_class, params = settings_url_parse(settings_url)
        params.update(kwargs)

        disable = params.pop("disable") if "disable" in params else not params.pop("enable", True)

        backend = backend_class(**params)
        if disable:
            backend.disable()
        self._add_backend(backend, middlewares, prefix)
        return backend

    def is_setup(self) -> bool:
        return bool(self._backends)

    def _check_setup(self) -> None:
        if not self._backends:
            raise NotConfiguredError("run `cache.setup(...)` before using cache")

    def _add_backend(self, backend: Backend, middlewares=(), prefix: str = default_prefix) -> None:
        self._backends[prefix] = (
            backend,
            middlewares + tuple(self._default_middlewares),
        )
        self._sorted_prefixes = tuple(sorted(self._backends.keys(), reverse=True))

    async def init(self, *args, **kwargs) -> None:
        if args or kwargs:
            self.setup(*args, **kwargs)
        for backend, _ in self._backends.values():
            await backend.init()

    @property
    def is_init(self) -> bool:
        return all(backend.is_init for backend, _ in self._backends.values())

    async def close(self) -> None:
        for backend, _ in self._backends.values():
            await backend.close()
