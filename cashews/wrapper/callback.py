import contextlib
import uuid
from typing import TYPE_CHECKING, Any, Iterator

from cashews._typing import AsyncCallable_T, Callback, Key, ShortCallback
from cashews.commands import PATTERN_CMDS, Command
from cashews.key import get_call_values

from .wrapper import Wrapper

if TYPE_CHECKING:  # pragma: no cover
    from cashews.backends.interface import Backend


class CallbackMiddleware:
    def __init__(self):
        self._callbacks = {}

    async def __call__(self, call: AsyncCallable_T, cmd: Command, backend: "Backend", *args, **kwargs):
        result = await call(*args, **kwargs)
        as_key = "pattern" if cmd in PATTERN_CMDS else "key"
        call_values = get_call_values(call, args, kwargs)
        key = call_values.get(as_key)
        if key is None or result is None:
            return result
        for callback in self._callbacks.values():
            await callback(cmd, key=key, result=result, backend=backend)
        return result

    def add_callback(self, callback: Callback, name: str):
        self._callbacks[name] = callback

    def remove_callback(self, name: str):
        del self._callbacks[name]

    @contextlib.contextmanager
    def callback(self, callback: Callback) -> Iterator[None]:
        name = uuid.uuid4().hex
        self.add_callback(callback, name)
        try:
            yield
        finally:
            self.remove_callback(name)


class CallbackWrapper(Wrapper):
    def __init__(self, name: str = ""):
        super().__init__(name)
        self.callbacks = CallbackMiddleware()
        self.add_middleware(self.callbacks)

    @contextlib.contextmanager
    def callback(self, callback: ShortCallback, cmd: Command) -> Iterator[None]:
        t_cmd = cmd

        async def _wrapped_callback(cmd: Command, key: Key, result: Any, backend: "Backend") -> None:
            if cmd == t_cmd:
                callback(key, result=result)

        with self.callbacks.callback(_wrapped_callback):
            yield
