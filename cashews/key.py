import inspect
from datetime import timedelta
from functools import lru_cache
from itertools import chain
from typing import Any, Callable, Dict, Optional, Tuple, Union
from unittest.mock import MagicMock

from .formatter import _ReplaceFormatter, default_formatter, template_to_pattern
from .typing import TTL


class WrongKeyException(ValueError):
    pass


class HDict(dict):
    def __hash__(self):
        return hash(frozenset(self.items()))


def ttl_to_seconds(ttl: Union[float, None, TTL]) -> Union[int, None, float]:
    timeout = ttl() if callable(ttl) else ttl
    if isinstance(timeout, timedelta):
        return timeout.total_seconds()
    if isinstance(timeout, str):
        return _ttl_from_str(timeout)
    return timeout


_STR_TO_DELTA = {
    "h": timedelta(hours=1),
    "m": timedelta(minutes=1),
    "s": timedelta(seconds=1),
    "d": timedelta(days=1),
}


def _ttl_from_str(ttl: str) -> int:
    result = 0
    mul = ""
    for char in ttl.strip().lower():
        if char.isdigit():
            mul += char
        elif char in _STR_TO_DELTA:
            result += int(mul) * _STR_TO_DELTA[char].total_seconds()
            mul = ""
        else:
            raise ValueError(f"ttl '{ttl}' has wrong string representation")
    if mul != "" and not result:
        return int(mul)
    return result


def get_cache_key(
    func: Callable,
    template: Optional[str] = None,
    args: Tuple[Any] = (),
    kwargs: Optional[Dict] = None,
) -> str:
    if not kwargs:
        return _get_cache_key(func, template, args, kwargs)
    try:
        kwargs = HDict(kwargs)
    except TypeError:
        return __get_cache_key(func, template, args, kwargs)
    else:
        return _get_cache_key(func, template, args, kwargs)


@lru_cache(maxsize=100)
def __get_cache_key(
    func: Callable,
    template: Optional[str] = None,
    args: Tuple[Any] = (),
    kwargs: Optional[HDict] = None,
):
    return _get_cache_key(func, template, args, kwargs)


def _get_cache_key(
    func: Callable,
    template: Optional[str] = None,
    args: Tuple[Any] = (),
    kwargs: Optional[Dict] = None,
) -> str:
    """
    Get cache key name for function (:param func) called with args and kwargs
    if func_args is passed key build with parameters are included in func_args dict or tuple otherwise use all of them
    Used function module and name as prefix if key parameter not passed
    :param func: Target function
    :param args: call positional arguments
    :param kwargs: call keyword arguments
    :return: cache key for call
    """
    kwargs = kwargs or {}
    key_values = get_call_values(func, args, kwargs)
    key_values = {k: v if v is not None else "" for k, v in key_values.items()}
    _key_template = template or get_cache_key_template(func)
    return template_to_pattern(_key_template, _formatter=default_formatter, **key_values).lower()


def get_func_params(func):
    signature = _get_func_signature(func)
    for param_name, param in signature.parameters.items():
        if param.kind not in [
            inspect.Parameter.VAR_KEYWORD,
            inspect.Parameter.VAR_POSITIONAL,
        ]:
            yield param_name


def get_cache_key_template(func: Callable, key: Optional[str] = None, prefix: str = "") -> str:
    """
    Get cache key name for function (:param func) called with args and kwargs
    Used function module and name as prefix if key parameter not passed
    :param func: Target function
    :param key: template for key, may contain alias to args or kwargs passed to a call
    :return: cache key template
    """
    func_params = get_func_params(func)
    if key is None:
        params = {param_name: "{" + param_name + "}" for param_name in func_params}
        key = ":".join([func.__module__, func.__name__, *chain(*params.items())]).lower()
    else:
        _check_key_params(key, func_params)
    if prefix:
        key = f"{prefix}:{key}"
    return key


class _Star:
    _STAR = "*"

    def __getattr__(self, item):
        return self._STAR

    def __getitem__(self, item):
        return self._STAR


def _check_key_params(key, func_params):
    func_params = {param: _Star() for param in func_params}
    errors = []

    def _default(name):
        errors.append(name)
        return "*"

    check = _ReplaceFormatter(default=_default)
    check.format(key, **func_params)
    if errors:
        raise WrongKeyException(f"Wrong parameter placeholder '{errors}' in the key ")


def get_call_values(func: Callable, args, kwargs) -> Dict:
    """
    Return dict with arguments and their values for function call with given positional and keywords arguments
    :param func: Target function
    :param args: call positional arguments
    :param kwargs: call keyword arguments
    :param func_args: arguments that will be included in results (transformation function for values if passed as dict)
    """
    key_values = {}
    for _key, _value in _get_call_values(func, args, kwargs).items():
        key_values[_key] = _value
        if isinstance(key_values[_key], bytes):
            key_values[_key] = key_values[_key].decode()
    return key_values


@lru_cache(maxsize=100)
def _get_func_signature(func):
    return inspect.signature(func)


def _get_call_values(func, args, kwargs):
    signature = _get_func_signature(func).bind(*args, **kwargs)
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
