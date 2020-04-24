import asyncio
from functools import wraps
from typing import Any, Callable, Dict, Optional, Union

from cashews.backends.interface import Backend
from cashews.key import get_call_values, get_func_params, get_templates_for, template_to_pattern


async def invalidate_func(backend: Backend, func, kwargs: Optional[Dict] = None):
    values = {**{param: "*" for param in get_func_params(func)}, **kwargs}
    values = {k: str(v) if v is not None else "" for k, v in values.items()}
    for template in get_templates_for(func):
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
            _args = get_call_values(func, args, kwargs, func_args=None)
            _args.update(defaults)
            for source, dest in args_map.items():
                if dest in _args:
                    _args[source] = _args.pop(dest)
                if callable(dest):
                    _args[source] = dest(*args, **kwargs)
            if callable(target):
                asyncio.create_task(invalidate_func(backend, target, _args))
            else:
                asyncio.create_task(
                    backend.delete_match(target.format({k: str(v) if v is not None else "" for k, v in _args.items()}))
                )
            return result

        return _wrap

    return _decor
