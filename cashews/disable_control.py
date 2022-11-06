from contextvars import ContextVar
from typing import List, Union

from .commands import Command


async def _is_disable_middleware(call, *args, backend=None, cmd=None, **kwargs):
    if backend.is_disable(cmd, Command.ALL):
        if cmd in (Command.GET, Command.GET_MANY):
            return kwargs.get("default", None)
        return None
    return await call(*args, **kwargs)


class ControlMixin:
    def __init__(self) -> None:
        self.__disable = ContextVar(str(id(self)), default=())

    @property
    def _disable(self) -> List[Command]:
        return list(self.__disable.get(()))

    def _set_disable(self, value: Union[bool, List[Command]]) -> None:
        if value is True:
            value = [
                Command.ALL,
            ]
        elif value is False:
            value = []
        self.__disable.set(tuple(value))

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
        _disable = self._disable
        if not cmds:
            _disable = [
                Command.ALL,
            ]
        if self._disable is False:
            _disable = []
        _disable.extend(cmds)
        self._set_disable(_disable)

    def enable(self, *cmds: Command) -> None:
        _disable = self._disable
        if not cmds:
            _disable = []
        for cmd in cmds:
            if cmd in _disable:
                _disable.remove(cmd)
        self._set_disable(_disable)
