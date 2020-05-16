from datetime import datetime, timedelta

from .key import get_call_values


def at(hour=23, minute=59, second=59, minimum: timedelta = timedelta(minutes=1), **kwargs):
    def _ttl():
        now = datetime.utcnow()
        expire_at = now.replace(hour=hour, minute=minute, second=second, **kwargs)
        delta = expire_at - now
        if delta < minimum:
            return minimum
        return delta

    return _ttl


async def _is_disable_middleware(call, *args, backend=None, cmd=None, **kwargs):
    if backend.is_disable(cmd, "cmds"):
        return None
    return await call(*args, **kwargs)


async def _auto_init(call, *args, backend=None, cmd=None, **kwargs):
    if not backend.is_init:
        await backend._init()
    return await call(*args, **kwargs)


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
