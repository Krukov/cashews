from contextvars import ContextVar
from typing import TYPE_CHECKING, Set

from ._typing import AsyncCallable_T
from .commands import ALL, Command

if TYPE_CHECKING:  # pragma: no cover
    from .backends.interface import Backend


async def _is_disable_middleware(call: AsyncCallable_T, cmd: Command, backend: "Backend", *args, **kwargs):
    if backend.is_disable(cmd):
        if cmd in (Command.GET, Command.GET_MANY):
            return kwargs.get("default", None)
        return None
    return await call(*args, **kwargs)


class ControlMixin:
    def __init__(self) -> None:
        self.__disable: ContextVar[Set[Command]] = ContextVar(str(id(self)), default=set())

    @property
    def _disable(self) -> Set[Command]:
        return self.__disable.get()

    def _set_disable(self, value: Set[Command]) -> None:
        self.__disable.set(value)

    def is_disable(self, *cmds: Command) -> bool:
        _disable = self._disable
        if not cmds and _disable:
            return True
        for cmd in cmds:
            if cmd in _disable:
                return True
        return False

    def is_enable(self, *cmds: Command) -> bool:
        return not self.is_disable(*cmds)

    def disable(self, *cmds: Command) -> None:
        if not cmds:
            _disable = ALL.copy()
        else:
            _disable = self._disable.copy()
            _disable.update(cmds)
        self._set_disable(_disable)

    def enable(self, *cmds: Command) -> None:
        if not cmds:
            _disable = set()
        else:
            _disable = self._disable.copy()
            _disable -= set(cmds)
        self._set_disable(_disable)
