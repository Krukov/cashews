import inspect
from itertools import chain
from typing import Any, Callable, Dict, Optional, Tuple

from .typing import FuncArgsType


def get_cache_key(
    func: Callable,
    args: Tuple[Any] = (),
    kwargs: Optional[Dict] = None,
    func_args: FuncArgsType = None,
    key: Optional[str] = None,
) -> str:
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
    kwargs = kwargs or {}
    key_values = get_call_values(func, args, kwargs, func_args)
    if key is None:
        key_values = {k: str(v) if v is not None else "" for k, v in key_values.items()}
        return ":".join([func.__module__, func.__name__, *chain(*key_values.items())]).lower()
    return key.format(**key_values).lower()


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
    key_values = {}
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
