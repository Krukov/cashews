from contextlib import contextmanager
from typing import TYPE_CHECKING

from cashews._typing import AsyncCallable_T
from cashews.commands import Command

from .wrapper import Wrapper

if TYPE_CHECKING:  # pragma: no cover
    from cashews.backends.interface import Backend


async def _is_disable_middleware(call: AsyncCallable_T, cmd: Command, backend: "Backend", *args, **kwargs):
    if backend.is_disable(cmd):
        if cmd in (Command.GET, Command.GET_MANY):
            return kwargs.get("default", None)
        return None
    return await call(*args, **kwargs)


class ControlWrapper(Wrapper):
    def __init__(self, name: str = ""):
        super().__init__(name)
        self.add_middleware(_is_disable_middleware)

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

    @property
    def is_full_disable(self):
        return all([backend.is_full_disable for backend, _ in self._backends.values()])
