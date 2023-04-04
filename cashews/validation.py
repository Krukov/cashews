import asyncio
import warnings
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from typing import Any, Callable, Dict, Optional, Union

from ._typing import AsyncCallable_T
from .backends.interface import _BackendInterface
from .commands import RETRIEVE_CMDS, Command
from .formatter import get_templates_for_func, template_to_pattern
from .key import get_call_values, get_func_params


async def invalidate_func(backend: _BackendInterface, func, kwargs: Optional[Dict] = None) -> None:
    warnings.warn(
        "invalidating by function object is deprecated. Use 'tags' feature instead", DeprecationWarning, stacklevel=2
    )
    values = {**{param: "*" for param in get_func_params(func)}, **kwargs}
    for template in get_templates_for_func(func):
        del_template = template_to_pattern(template, **values)
        await backend.delete_match(del_template)


def invalidate(
    backend: _BackendInterface,
    target: Union[str, Callable],
    args_map: Optional[Dict[str, str]] = None,
    defaults: Optional[Dict[str, Any]] = None,
):
    args_map = args_map or {}
    defaults = defaults or {}

    def _decor(func: AsyncCallable_T) -> AsyncCallable_T:
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


@contextmanager
def invalidate_further():
    _INVALIDATE_FURTHER.set(True)
    try:
        yield
    finally:
        _INVALIDATE_FURTHER.set(False)


async def _aiter(num=0):  # pragma: no cover
    """A trick for typing"""
    for i in range(num):
        yield i


async def _invalidate_middleware(call, cmd: Command, backend: _BackendInterface, *args, **kwargs):
    if _INVALIDATE_FURTHER.get() and cmd in RETRIEVE_CMDS:
        if "key" in kwargs:
            await backend.delete(kwargs["key"])
            return kwargs.get("default")
        if cmd == Command.GET_MATCH:
            await backend.delete_match(kwargs["pattern"])
            return _aiter()
        if cmd == Command.GET_MANY:
            await backend.delete_many(*args)
            return ()
    return await call(*args, **kwargs)
