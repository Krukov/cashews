from functools import partial
from typing import Dict, List, Tuple

from cashews import validation
from cashews._typing import Key, Middleware
from cashews.backends.interface import Backend
from cashews.commands import Command
from cashews.exceptions import NotConfiguredError

from .auto_init import create_auto_init
from .backend_settings import settings_url_parse


class Wrapper:
    default_prefix = ""

    def __init__(self, name: str = ""):
        self._backends: Dict[str, Tuple[Backend, Tuple[Middleware, ...]]] = {}
        self._default_middlewares: List[Middleware, ...] = [
            create_auto_init(),
            validation._invalidate_middleware,
        ]
        self.name = name
        super().__init__()

    def add_middleware(self, middleware: Middleware):
        self._default_middlewares.append(middleware)

    def _get_backend_and_config(self, key: Key) -> Tuple[Backend, Tuple[Middleware, ...]]:
        for prefix in sorted(self._backends.keys(), reverse=True):
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

    def is_setup(self) -> bool:
        return bool(self._backends)

    def _check_setup(self):
        if not self._backends:
            raise NotConfiguredError("run `cache.setup(...)` before using cache")

    def _add_backend(self, backend: Backend, middlewares=(), prefix: str = default_prefix):
        self._backends[prefix] = (
            backend,
            tuple(self._default_middlewares) + middlewares,
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

    async def close(self):
        for backend, _ in self._backends.values():
            await backend.close()
