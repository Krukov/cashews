import asyncio
from contextvars import ContextVar
from functools import wraps
from typing import Any, Callable, Dict, Optional, Union

from .backends.interface import Backend
from .formatter import get_templates_for_func, template_to_pattern
from .key import get_call_values, get_func_params


async def invalidate_func(backend: Backend, func, kwargs: Optional[Dict] = None):
    values = {**{param: "*" for param in get_func_params(func)}, **kwargs}
    for template in get_templates_for_func(func):
        del_template = template_to_pattern(template, **values)
        await backend.delete_match(del_template)


def invalidate(
    backend: Backend,
    target: Union[str, Callable],
    args_map: Optional[Dict[str, str]] = None,
    defaults: Optional[Dict[str, Any]] = None,
):
    args_map = args_map or {}
    defaults = defaults or {}

    def _decor(func):
        @wraps(func)
        async def _wrap(*args, **kwargs):
            result = await func(*args, **kwargs)
            _args = get_call_values(func, args, kwargs)
            _args.update(defaults)
            for source, dest in args_map.items():
                if dest in _args:
                    _args[source] = _args.pop(dest)
            if callable(target):
                asyncio.create_task(invalidate_func(backend, target, _args))
            else:
                key = target.format(**{k: str(v) if v is not None else "" for k, v in _args.items()})
                asyncio.create_task(backend.delete_match(key))
            return result

        return _wrap

    return _decor


_INVALIDATE_FURTHER = ContextVar("invalidate", default=False)


def set_invalidate_further():
    _INVALIDATE_FURTHER.set(True)


async def _invalidate_middleware(call, *args, key=None, backend=None, cmd=None, **kwargs):
    if _INVALIDATE_FURTHER.get() and key is not None and cmd != "delete":
        asyncio.create_task(backend.delete(key))
        _INVALIDATE_FURTHER.set(False)
        return None
    if key is None:
        return await call(*args, **kwargs)
    return await call(*args, key=key, **kwargs)
