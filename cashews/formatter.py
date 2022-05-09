import base64
import json
import re
from hashlib import md5, sha1, sha256
from string import Formatter
from typing import Callable, Iterable, Optional, Tuple


def _decode_bytes(value: bytes):
    try:
        return value.decode()
    except UnicodeDecodeError:
        return value.hex()


def _get_decode_array(format_value):
    def _decode_array(values: Iterable[str]) -> str:
        return ":".join([format_value(value) for value in values])

    return _decode_array


def _get_decoded_dict(format_value):
    def _decode_dict(value: dict):
        return ":".join(f"{k}:{format_value(v)}" for k, v in sorted(value.items()))

    return _decode_dict


class _ReplaceFormatter(Formatter):
    def __init__(self, default=lambda field: "*"):
        self.__default = default
        _decode_array = _get_decode_array(self._format_field)
        self.__type_format = {
            bool: lambda value: str(value).lower(),
            bytes: _decode_bytes,
            tuple: _decode_array,
            list: _decode_array,
            set: _decode_array,
            dict: _get_decoded_dict(self._format_field),
        }
        super().__init__()

    def set_format_for_type(self, type, format_function):
        self.__type_format[type] = format_function

    def type_format(self, type):
        def _decorator(func):
            self.set_format_for_type(type, func)
            return func

        return _decorator

    def get_field(self, field_name, args, kwargs):
        try:
            return super().get_field(field_name, args, kwargs)
        except (KeyError, AttributeError):
            return self.__default(field_name), None

    def _format_field(self, value):
        if value is None:
            return ""
        _type = type(value)
        if _type in self.__type_format:
            return self.__type_format[_type](value)
        for _type, func_format in self.__type_format.items():
            if isinstance(value, _type):
                return func_format(value)
        return str(value)

    def format_field(self, value, format_spec):
        return format(self._format_field(value))


class _FuncFormatter(_ReplaceFormatter):
    def __init__(self, *args, **kwargs):
        self._functions = {}
        super().__init__(*args, **kwargs)

    def _register(self, alias, function):
        self._functions[alias] = function

    def register(self, alias):
        def _decorator(func):
            self._register(alias, func)
            return func

        return _decorator

    def format_field(self, value, format_spec):
        format_spec, args = self.parse_format_spec(format_spec)
        if format_spec in self._functions:
            value = self._functions[format_spec](value, *args)
        value = super().format_field(value, format_spec if format_spec not in self._functions else "")
        return value

    @staticmethod
    def parse_format_spec(format_spec):
        if not format_spec or "(" not in format_spec:
            return format_spec, ()
        format_spec, args = format_spec.split("(", 1)
        return format_spec, args.replace(")", "").split(",")


default_formatter = _FuncFormatter(lambda name: "")
default_formatter._register("len", len)


@default_formatter.register("jwt")
def _jwt_func(jwt: str, key: str):
    _, payload, _ = jwt.split(".", 2)
    payload_dict = json.loads(base64.b64decode(payload))
    return payload_dict.get(key)


@default_formatter.register("hash")
def _hash_func(value: str, alg="md5") -> str:
    algs = {"sha1": sha1, "md5": md5, "sha256": sha256}
    alg = algs[alg]
    return alg(value.encode()).hexdigest()


def template_to_pattern(template: str, _formatter=_ReplaceFormatter(), **values) -> str:
    return _formatter.format(template, **values)


def _re_default(field_name):
    return f"(?P<{field_name.replace('.', '_')}>[^:]*)"


_re_formatter = _ReplaceFormatter(default=_re_default)
_REGISTER = {}


def register_template(func, template: str):
    pattern = "(.*[:])?" + template_to_pattern(template, _formatter=_re_formatter) + "$"
    compile_pattern = re.compile(pattern, flags=re.MULTILINE)
    _REGISTER.setdefault((func.__module__ or "", func.__name__), set()).add((template, compile_pattern))


def get_templates_for_func(func):
    return (template for template, _ in _REGISTER.get((func.__module__ or "", func.__name__), set()))


def get_template_and_func_for(key: str) -> Tuple[Optional[str], Optional[Callable]]:
    for func, templates in _REGISTER.items():
        for template, compile_pattern in templates:
            if compile_pattern.fullmatch(key):
                return template_to_pattern(template), func
    return None, None


def get_template_for_key(key: str) -> Tuple[Optional[str], Optional[dict]]:
    for func, templates in _REGISTER.items():
        for template, compile_pattern in templates:
            match = compile_pattern.fullmatch(key)
            if match:
                return template, match.groupdict()
    return None, None
