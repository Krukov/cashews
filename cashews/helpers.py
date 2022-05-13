from .key import get_call_values
from .utils import get_obj_size


def add_prefix(prefix: str):
    async def _middleware(call, *args, backend=None, cmd=None, **kwargs):
        if cmd.lower() == "get_many":
            return await call(*[prefix + key for key in args])
        call_values = get_call_values(call, args, kwargs)
        as_key = "key"
        if cmd in ["delete_match", "get_match", "keys_match"]:
            as_key = "pattern"
        key = call_values.get(as_key)
        if key:
            call_values[as_key] = prefix + key
            return await call(**call_values)
        return await call(*args, **kwargs)

    return _middleware


def all_keys_lower():
    async def _middleware(call, *args, backend=None, cmd=None, **kwargs):
        if cmd.lower() == "get_many":
            return await call(*[key.lower() for key in args])
        call_values = get_call_values(call, args, kwargs)
        as_key = "key"
        if cmd == "delete_match":
            as_key = "pattern"
        key = call_values.get(as_key)
        if key:
            call_values[as_key] = key.lower()
            return await call(**call_values)
        return await call(*args, **kwargs)

    return _middleware


def memory_limit(min=0, max=None):
    async def _memory_middleware(call, *args, backend=None, cmd=None, **kwargs):
        if cmd != "set":
            return await call(*args, **kwargs)
        call_values = get_call_values(call, args, kwargs)
        value_size = get_obj_size(call_values["value"])
        if max and value_size > max or value_size < min:
            return None
        return await call(*args, **kwargs)

    return _memory_middleware
