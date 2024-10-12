import base64
import json
import re
import warnings
from contextlib import contextmanager
from hashlib import md5, sha1, sha256
from string import Formatter
from typing import Any, Callable, Dict, Iterable, Pattern, Tuple, TypeVar

from . import key_context
from ._typing import KeyOrTemplate, KeyTemplate
from .exceptions import WrongKeyError

TemplateValue = str
T = TypeVar("T")
_re_special_chars_map = {i: "\\" + chr(i) for i in b"()[]?*+-|^$\\&~# \t\n\r\v\f"}


def _decode_bytes(value: bytes):
    try:
        return value.decode()
    except UnicodeDecodeError:
        return value.hex()


def _decode_exception(value: Exception):
    return f"{value.__class__.__name__}:{value}"


def _get_decode_array(format_value):
    def _decode_array(values: Iterable[str]) -> TemplateValue:
        return ":".join([format_value(value) for value in values])

    return _decode_array


def _get_decoded_dict(format_value):
    def _decode_dict(value: dict) -> TemplateValue:
        _kv = (k + ":" + format_value(v) for k, v in sorted(value.items()))
        return ":".join(_kv)

    return _decode_dict


def _decoded_bool(value: bool) -> TemplateValue:
    return str(value).lower()


def _decode_direct(value: str) -> TemplateValue:
    return value


class _ReplaceFormatter(Formatter):
    def __init__(self, default=lambda field: "*"):
        self.__default = default
        _decode_array = _get_decode_array(self._format_field)
        self.__type_format = {
            str: _decode_direct,
            bool: _decoded_bool,
            bytes: _decode_bytes,
            Exception: _decode_exception,
            tuple: _decode_array,
            list: _decode_array,
            set: _decode_array,
            dict: _get_decoded_dict(self._format_field),
        }
        super().__init__()

    @contextmanager
    def default(self, _default):
        was = self.__default
        self.__default = _default
        try:
            yield
        finally:
            self.__default = was

    def set_format_for_type(self, value, format_function):
        self.__type_format[value] = format_function

    def type_format(self, value):
        def _decorator(func):
            self.set_format_for_type(value, func)
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
        for _type_map, func_format in self.__type_format.items():
            if isinstance(value, _type_map):
                return func_format(value)
        return str(value)

    def format_field(self, value, format_spec):
        return format(self._format_field(value))


class _KeyFormatter(_ReplaceFormatter):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._functions: Dict[str, Tuple[Callable, bool]] = {}
        self._final_formatters = []
        super().__init__(*args, **kwargs)

    def register_func(self, alias: str, preformat: bool = True) -> Callable[[T], T]:
        def _decorator(func):
            self._register(alias, func, preformat=preformat)
            return func

        return _decorator

    def register(self, alias: str, preformat: bool = True):
        warnings.warn(
            "`register` function was renamed to `register_func` and will be removed in next release",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.register_func(alias, preformat=preformat)

    def _register(self, alias: str, function: Callable, preformat: bool = True) -> None:
        self._functions[alias] = (function, preformat)

    def add_formatter(self, formatter: Callable[[str, ...], str]) -> None:
        self._final_formatters.append(formatter)

    def format_field(self, value: Any, format_spec: str) -> TemplateValue:
        if format_spec == "":
            return super().format_field(value, format_spec)
        format_spec, args = self.parse_format_spec(format_spec)
        if format_spec not in self._functions:
            raise WrongKeyError(f"Invalid key format function {format_spec}")
        func, preformat = self._functions[format_spec]
        if preformat:
            value = super().format_field(value, "")
        return super().format_field(func(value, *args), "")

    @staticmethod
    def parse_format_spec(format_spec: str):
        if not format_spec or "(" not in format_spec:
            return format_spec, ()
        format_spec, args = format_spec.split("(", 1)
        return format_spec, args.replace(")", "").split(",")

    def vformat(self, format_string, args, kwargs):
        key = super().vformat(format_string, args, kwargs)
        for formatter in self._final_formatters:
            key = formatter(key)
        return key


default_formatter = _KeyFormatter(lambda name: "")


@default_formatter.register_func("len")
def _len(value: TemplateValue):
    return str(len(value))


@default_formatter.register_func("get", preformat=False)
def _get(value: Any, key: str) -> TemplateValue:
    return value.get(key)


@default_formatter.register_func("jwt")
def _jwt_func(jwt: TemplateValue, key: str) -> TemplateValue:
    _, payload, _ = jwt.split(".", 2)
    payload_dict = json.loads(base64.b64decode(payload))
    return payload_dict.get(key)


@default_formatter.register_func("hash")
def _hash_func(value: TemplateValue, alg="md5") -> TemplateValue:
    algs = {"sha1": sha1, "md5": md5, "sha256": sha256}
    alg = algs[alg]
    return alg(value.encode()).hexdigest()


@default_formatter.register_func("lower")
def _lower(value: TemplateValue) -> TemplateValue:
    return value.lower()


@default_formatter.register_func("upper")
def _upper(value: TemplateValue) -> TemplateValue:
    return value.upper()


def default_format(template: KeyTemplate, **values) -> KeyOrTemplate:
    _template_context, rewrite = key_context.get()
    _template_context = {**values, "@": _template_context}
    return default_formatter.format(template, **_template_context)


def _re_default(field_name):
    field_name = field_name.split(".")[0]
    return f"(?P<{field_name}>.+)?"


_re_formatter = _ReplaceFormatter(default=_re_default)


#  TODO: remove
def template_to_re_pattern(template: KeyTemplate) -> Pattern:
    pattern = _re_formatter.format(template.translate(_re_special_chars_map))
    return re.compile("^" + pattern + "$", flags=re.MULTILINE)
