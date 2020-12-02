from contextvars import ContextVar

_ALL = "_"


async def _is_disable_middleware(call, *args, backend=None, cmd=None, **kwargs):

    if backend.is_disable(cmd, _ALL):
        if cmd == "get":
            return kwargs.get("default")
        return None
    return await call(*args, **kwargs)


class ControlMixin:
    def __init__(self, *args, **kwargs):
        self.__disable = ContextVar(str(id(self)), default=())
        if "disable" in kwargs:
            self._set_disable(kwargs.pop("disable"))
        else:
            self._set_disable(not kwargs.pop("enable", True))
        super().__init__(*args, **kwargs)

    @property
    def _disable(self):
        return list(self.__disable.get(()))

    def _set_disable(self, value):
        if value is True:
            value = [
                _ALL,
            ]
        elif value is False:
            value = []
        self.__disable.set(tuple(value))

    def is_disable(self, *cmds: str) -> bool:
        _disable = self._disable
        if not cmds and _disable:
            return True
        for cmd in cmds:
            if cmd.lower() in [c.lower() for c in _disable]:
                return True
        return False

    def is_enable(self, *cmds):
        return not self.is_disable(*cmds)

    def disable(self, *cmds: str):
        _disable = self._disable
        if not cmds:
            _disable = [
                _ALL,
            ]
        if self._disable is False:
            _disable = []
        _disable.extend(cmds)
        self._set_disable(_disable)

    def enable(self, *cmds: str):
        _disable = self._disable
        if not cmds:
            _disable = []
        for cmd in cmds:
            if cmd in _disable:
                _disable.remove(cmd)
        self._set_disable(_disable)
