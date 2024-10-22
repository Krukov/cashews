from __future__ import annotations

import inspect
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Callable, Container, Iterable

from .exceptions import WrongKeyError
from .formatter import default_format, default_formatter

if TYPE_CHECKING:  # pragma: no cover
    from ._typing import Key, KeyOrTemplate, KeyTemplate

_KWARGS = "__kwargs__"
_ARGS = "__args__"
_ARGS_KWARGS = (_ARGS, _KWARGS)
Args = tuple[Any, ...]
Kwargs = dict[str, Any]


def get_cache_key(
    func: Callable,
    template: KeyTemplate | None = None,
    args: Args = (),
    kwargs: Kwargs | None = None,
) -> Key:
    """
    Get cache key name for function (:param func) called with args and kwargs
    if func_args is passed key build with parameters are included in func_args dict or tuple otherwise use all of them
    Used function module and name as prefix if key parameter not passed
    :param func: Target function
    :param template: precompile template
    :param args: call positional arguments
    :param kwargs: call keyword arguments
    :return: cache key for call
    """
    kwargs = kwargs or {}
    if not args and template and _KWARGS not in template and _ARGS not in template:
        key_values = kwargs
    else:
        key_values = _get_call_values(func, args, kwargs)
    _key_template = template or get_cache_key_template(func)
    return default_format(_key_template, **key_values)


def get_func_params(func: Callable) -> Iterable[str]:
    signature = _get_func_signature(func)
    for param_name, param in signature.parameters.items():
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            yield _KWARGS
        elif param.kind == inspect.Parameter.VAR_POSITIONAL:
            yield _ARGS
        else:
            yield param_name


@lru_cache(maxsize=10000)
def get_cache_key_template(
    func: Callable,
    key: KeyOrTemplate | None = None,
    prefix: str = "",
    exclude_parameters: Container = (),
) -> KeyOrTemplate:
    """
    Get cache key name for function (:param func) called with args and kwargs
    Used function module and name as prefix if key parameter not passed
    :param func: Target function
    :param key: template for key, may contain alias to args or kwargs passed to a call
    :param prefix: a prefix
    :param exclude_parameters: array of `args` and `kwargs` names to exclude from a key
        template (if key parameter not passed)
    :return: cache key template
    """

    if key is None:
        key = generate_key_template(func, exclude_parameters)
    else:
        if "{" in key:
            _check_key_params(key, get_func_params(func))
    if prefix:
        key = f"{prefix}:{key}"
    return key


def generate_key_template(func: Callable, exclude_parameters: Container = ()) -> KeyTemplate:
    """
    Generate template for function (:param func) called with args and kwargs
    Used function module and name as prefix if key parameter not passed
    :param func: Target function
    :param exclude_parameters: array of `args` and `kwargs` names to exclude
        from a key template (if key parameter not passed)
    :return: cache key template
    """
    func_params = tuple(get_func_params(func))
    key_template = f"{func.__module__}:{func.__name__}"
    if func_params and func_params[0] == "self":
        key_template = f"{func.__module__}:{func.__qualname__}"
    for param_name in func_params:
        if param_name in exclude_parameters:
            continue
        if param_name in _ARGS_KWARGS:
            key_template += f":{{{param_name}}}"
        else:
            key_template += f":{param_name}:{{{param_name}}}"
    return key_template


class _Star:
    def __getattr__(self, item):
        return _Star()

    def __getitem__(self, item):
        return _Star()


def _check_key_params(key: KeyOrTemplate, func_params: Iterable[str]):
    func_params = {param: _Star() for param in func_params}

    def _default(name):
        raise WrongKeyError(f"Wrong parameter placeholder '{name}' in the key ")

    with default_formatter.default(_default):
        default_format(key, **func_params)


def get_call_values(func: Callable, args: Args, kwargs: Kwargs) -> dict:
    """
    Return dict with arguments and their values for function call with given positional and keywords arguments
    :param func: Target function
    :param args: call positional arguments
    :param kwargs: call keyword arguments
    """
    key_values = {}
    for _key, _value in _get_call_values(func, args, kwargs).items():
        if _key not in _ARGS_KWARGS:
            key_values[_key] = _value
    return key_values


@lru_cache(maxsize=1000)
def _get_func_signature(func: Callable):
    return inspect.signature(func)


def _get_call_values(func: Callable, args: Args, kwargs: Kwargs):
    if len(args) == 0:
        _kwargs = {**kwargs}
        for name, parameter in _get_func_signature(func).parameters.items():
            if parameter.kind != inspect.Parameter.VAR_KEYWORD and name in _kwargs:
                del _kwargs[name]
        return {**kwargs, _KWARGS: _kwargs}

    signature = _get_func_signature(func).bind(*args, **kwargs)
    signature.apply_defaults()
    result = {}
    for _name, _value in signature.arguments.items():
        parameter: inspect.Parameter = signature.signature.parameters[_name]  # type: ignore[no-redef]
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            result[_KWARGS] = _value
            result.update(_value)
        elif parameter.kind == inspect.Parameter.VAR_POSITIONAL:
            result[_ARGS] = _value
        else:
            result[_name] = _value
    return result


def noself(decor_func):
    def _decor(*args, **kwargs):
        def outer(method):
            if "key" not in kwargs:
                kwargs["key"] = get_cache_key_template(method, exclude_parameters=("self",))
            return decor_func(*args, **kwargs)(method)

        return outer

    return _decor
