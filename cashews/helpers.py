from .key import get_call_values


def add_prefix(prefix: str):
    async def _middleware(call, *args, backend=None, cmd=None, **kwargs):
        if cmd.lower() == "get_many":
            return await call(*[prefix + key for key in args])
        call_values = get_call_values(call, args, kwargs)
        as_key = "key"
        if cmd == "delete_match":
            as_key = "pattern"
        key = call_values.get(as_key)
        if key:
            call_values[as_key] = prefix + key
            return await call(**call_values)
        return await call(*args, **kwargs)

    return _middleware
