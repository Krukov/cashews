import inspect
import re
from datetime import timedelta
from itertools import chain
from string import Formatter
from typing import Any, Callable, Dict, Optional, Tuple, Union

from .typing import TTL


class WrongKeyException(ValueError):
    pass


def ttl_to_seconds(ttl: Union[float, None, TTL]) -> Union[int, None, float]:
    timeout = ttl() if callable(ttl) else ttl
    return timeout.total_seconds() if isinstance(timeout, timedelta) else timeout


def get_cache_key(
    func: Callable, template: Optional[str] = None, args: Tuple[Any] = (), kwargs: Optional[Dict] = None,
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
    return template_to_pattern(_key_template, _formatter=_Blank(""), **key_values).lower()


def get_func_params(func):
    signature = inspect.signature(func)
    for param_name, param in signature.parameters.items():
        if param.kind not in [inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL]:
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


def _check_key_params(key, func_params):
    func_params = {param: param for param in func_params}
    check = _CheckFormatter()
    check.format(key, **func_params)
    if check.error:
        raise WrongKeyException(f"Wrong parameter placeholder '{check.error}' in the key ")


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
    def __init__(self, reg_field=lambda f: f):
        self._reg_field = reg_field
        super().__init__()

    def get_field(self, field_name, args, kwargs):
        try:
            return super().get_field(field_name, args, kwargs)
        except (KeyError, AttributeError):
            self._reg_field(field_name)
            return f"(?P<{field_name.replace('.', '_')}>[^:]*)", None


class _Blank(Formatter):
    def __init__(self, default="*"):
        self.__default = default
        super().__init__()

    def get_field(self, field_name, args, kwargs):
        try:
            return super().get_field(field_name, args, kwargs)
        except (KeyError, AttributeError):
            return self.__default, None


class _CheckFormatter(Formatter):
    def __init__(self):
        self.error = False
        super().__init__()

    def get_field(self, field_name, args, kwargs):
        try:
            return super().get_field(field_name, args, kwargs)
        except KeyError:
            self.error = field_name
            return "", None
        except AttributeError:
            return "", None


def template_to_pattern(template: str, _formatter=_Blank(), **values) -> str:
    return _formatter.format(template, **values)


_REGISTER = {}


def register_template(func, template: str):
    fields = []
    pattern = "(.*[:])?" + template_to_pattern(template, _formatter=_ReFormatter(fields.append)) + "$"
    compile_pattern = re.compile(pattern, flags=re.MULTILINE)
    _REGISTER.setdefault((func.__module__ or "", func.__name__), set()).add((template, compile_pattern, tuple(fields)))


def get_templates_for_func(func):
    return (template for template, _, _ in _REGISTER.get((func.__module__ or "", func.__name__), set()))


def get_template_and_func_for(key: str) -> Tuple[Optional[str], Optional[Callable]]:
    for func, templates in _REGISTER.items():
        for template, compile_pattern, _ in templates:
            if compile_pattern.fullmatch(key):
                return template_to_pattern(template), func
    return None, None


def get_template_for_key(key: str) -> Tuple[Optional[str], Optional[dict]]:
    for func, templates in _REGISTER.items():
        for template, compile_pattern, _ in templates:
            match = compile_pattern.fullmatch(key)
            if match:
                return template, match.groupdict()
    return None, None
