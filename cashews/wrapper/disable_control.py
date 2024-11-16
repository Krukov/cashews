from __future__ import annotations

from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Iterator

from cashews.commands import ALL, Command
from cashews.exceptions import NotConfiguredError

from .wrapper import Wrapper

if TYPE_CHECKING:  # pragma: no cover
    from cashews._typing import AsyncCallable_T
    from cashews.backends.interface import Backend

_CONTEXT_NAME = "_control"


class ControlWrapper(Wrapper):
    def __init__(self, name: str = ""):
        super().__init__(name)
        self.__disable = False
        self._set_cache_context(_CONTEXT_NAME, value=set())
        self.add_middleware(self._is_disable_middleware)

    @property
    def _disable(self):
        return self._get_cache_context_value(_CONTEXT_NAME) or set()

    def _set_disable(self, value):
        self._set_cache_context(_CONTEXT_NAME, value=value)

    async def _is_disable_middleware(self, call: AsyncCallable_T, cmd: Command, backend: Backend, *args, **kwargs):
        if self.is_disable(cmd):
            if cmd in (Command.GET, Command.GET_MANY):
                return kwargs.get("default", None)
            return None
        return await call(*args, **kwargs)

    def disable(self, *cmds: Command, temporary: bool = False) -> None:
        if not cmds:
            if not temporary:
                self.__disable = True
            else:
                self._set_disable(ALL)
        else:
            if not temporary:
                self.__disable = False
            _disable = self._disable.copy()
            _disable.update(cmds)
            self._set_disable(_disable)

    def enable(self, *cmds: Command, temporary: bool = False) -> None:
        if not temporary:
            self.__disable = False
        if cmds:
            _disable = self._disable.copy()
            _disable -= set(cmds)
            self._set_disable(_disable)
        else:
            self._set_disable(None)

    @contextmanager
    def disabling(self, *cmds: Command) -> Iterator[None]:
        self.disable(*cmds, temporary=True)
        try:
            yield
        finally:
            with suppress(NotConfiguredError):
                self.enable(*cmds, temporary=True)

    def is_disable(self, *cmds: Command) -> bool:
        if self.__disable:
            return True
        return self._is_disable(*cmds)

    def _is_disable(self, *cmds: Command) -> bool:
        _disable = self._disable
        if not cmds and _disable:
            return True
        for cmd in cmds:
            if cmd in _disable:
                return True
        return False

    def is_enable(self, *cmds: Command) -> bool:
        return not self.is_disable(*cmds)

    @property
    def is_full_disable(self) -> bool:
        return self.__disable
