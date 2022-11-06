from .commands import PATTERN_CMDS, Command
from .key import get_call_values
from .utils import get_obj_size


def add_prefix(prefix: str):
    async def _middleware(call, *args, backend=None, cmd=None, **kwargs):
        if cmd == Command.GET_MANY:
            return await call(*[prefix + key for key in args])
        call_values = get_call_values(call, args, kwargs)
        if cmd == Command.SET_MANY:
            call_values["pairs"] = {prefix + key: value for key, value in call_values["pairs"]}
            return await call(**call_values)

        as_key = "pattern" if cmd in PATTERN_CMDS else "key"
        key = call_values.get(as_key)
        if key:
            call_values[as_key] = prefix + key
            return await call(**call_values)
        return await call(*args, **kwargs)

    return _middleware


def all_keys_lower():
    async def _middleware(call, *args, backend=None, cmd=None, **kwargs):
        if cmd == Command.GET_MANY:
            return await call(*[key.lower() for key in args])
        call_values = get_call_values(call, args, kwargs)

        if cmd == Command.SET_MANY:
            call_values["pairs"] = {key.lower(): value for key, value in call_values["pairs"]}
            return await call(**call_values)

        as_key = "pattern" if cmd in PATTERN_CMDS else "key"

        key = call_values.get(as_key)
        if key:
            call_values[as_key] = key.lower()
            return await call(**call_values)
        return await call(*args, **kwargs)

    return _middleware


def memory_limit(min_bytes=0, max_bytes=None):
    async def _memory_middleware(call, *args, backend=None, cmd=None, **kwargs):
        if cmd != Command.SET:
            return await call(*args, **kwargs)
        call_values = get_call_values(call, args, kwargs)
        value_size = get_obj_size(call_values["value"])
        if max_bytes and value_size > max_bytes or value_size < min_bytes:
            return None
        return await call(*args, **kwargs)

    return _memory_middleware
