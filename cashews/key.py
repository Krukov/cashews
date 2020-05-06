import inspect
import re
from datetime import timedelta
from itertools import chain
from string import Formatter
from typing import Any, Callable, Dict, Optional, Tuple, Union

from .typing import TTL, FuncArgsType


def ttl_to_seconds(ttl: Union[float, None, TTL]) -> Union[int, None, float]:
    timeout = ttl() if callable(ttl) else ttl
    return timeout.total_seconds() if isinstance(timeout, timedelta) else timeout


def get_cache_key(
    func: Callable,
    template: str = None,
    args: Tuple[Any] = (),
    kwargs: Optional[Dict] = None,
    func_args: FuncArgsType = None,
) -> str:
    """
    Get cache key name for function (:param func) called with args and kwargs
    if func_args is passed key build with parameters are included in func_args dict or tuple otherwise use all of them
    Used function module and name as prefix if key parameter not passed
    :param func: Target function
    :param args: call positional arguments
    :param kwargs: call keyword arguments
    :param func_args: arguments that will be used in key
    :return: cache key for call
    """
    kwargs = kwargs or {}
    key_values = get_call_values(func, args, kwargs, func_args)
    key_values = {k: str(v) if v is not None else "" for k, v in key_values.items()}
    _key_template = template or get_cache_key_template(func, func_args)
    return _key_template.format(**key_values).lower()


def get_func_params(func):
    signature = inspect.signature(func)
    for param_name, param in signature.parameters.items():
        if param.kind not in [inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL]:
            yield param_name


def get_cache_key_template(func: Callable, func_args: FuncArgsType = None, key: Optional[str] = None) -> str:
    """
    Get cache key name for function (:param func) called with args and kwargs
    if func_args is passed key build with parameters are included in func_args dict or tuple otherwise use all of them
    Used function module and name as prefix if key parameter not passed
    :param func: Target function
    :param args: call positional arguments
    :param kwargs: call keyword arguments
    :param func_args: arguments that will be used in key
    :param key: template for key, may contain alias to args or kwargs passed to a call
    :return: cache key for call
    """
    if key:
        return key

    if func_args is not None:
        params = {param: "{" + param + "}" for param in func_args}
    else:
        params = {param_name: "{" + param_name + "}" for param_name in get_func_params(func)}
    return ":".join([func.__module__, func.__name__, *chain(*params.items())]).lower()


def get_call_values(func: Callable, args, kwargs, func_args: FuncArgsType) -> Dict:
    """
    Return dict with arguments and their values for function call with given positional and keywords arguments
    :param func: Target function
    :param args: call positional arguments
    :param kwargs: call keyword arguments
    :param func_args: arguments that will be included in results (transformation function for values if passed as dict)
    """
    key_values = {key: "" for key in func_args or []}
    for _key, _value in _get_call_values(func, args, kwargs).items():
        if func_args is None or _key in func_args:
            key_values[_key] = _value
            if isinstance(func_args, dict) and callable(func_args[_key]):
                key_values[_key] = func_args[_key](_value)
            if isinstance(key_values[_key], bytes):
                key_values[_key] = key_values[_key].decode()
    return key_values


def _get_call_values(func, args, kwargs):
    signature = inspect.signature(func).bind(*args, **kwargs)
    signature.apply_defaults()
    result = {}
    for _name, _value in signature.arguments.items():
        parameter: inspect.Parameter = signature.signature.parameters[_name]
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            result.update(_value)
        elif parameter.kind == inspect.Parameter.VAR_POSITIONAL:
            continue
        else:
            result[_name] = _value
    return result


class _ReFormatter(Formatter):
    def get_value(self, key, args, kwargs):
        try:
            return kwargs[key]
        except KeyError:
            return f"(?P<{key}>.*)"


class _Star(Formatter):
    def get_value(self, key, args, kwargs):
        try:
            return kwargs[key]
        except KeyError:
            return "*"


def template_to_pattern(template: str, _formatter=_Star(), **values):
    return _formatter.format(template, **values)


_REGISTER = {}


def register_template(func, template: str):
    pattern = template_to_pattern(template, _formatter=_ReFormatter())
    compile_pattern = re.compile(pattern)
    _REGISTER.setdefault((func.__module__, func.__name__), set()).add((template, compile_pattern))


def get_templates_for(func):
    return (template for template, _ in _REGISTER.get((func.__module__, func.__name__), set()))


def get_template_and_func_for(key: str):
    for func, templates in _REGISTER.items():
        for template, compile_pattern in templates:
            if compile_pattern.match(key):
                return template_to_pattern(template), func
