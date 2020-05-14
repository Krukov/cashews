from contextvars import ContextVar
from typing import Any

from ...typing import CacheCondition

_empty = object()


def _default_store_condition(result, args, kwargs) -> bool:
    return result is not None


def _store_all(result, args, kwargs) -> bool:
    return True


def _get_cache_condition(condition: CacheCondition):
    if condition is None:
        return _default_store_condition
    if condition in [Any, "all", any, "any"]:
        return _store_all
    return condition


class CacheDetect:
    def __init__(self):
        self._value = {}

    def set(self, key: str, **kwargs):
        self._value.setdefault(key, []).append(kwargs)

    def get(self):
        return dict(self._value)

    def merge(self, other):
        self._value.update(other._value)


_var = ContextVar("cashews", default=None)


class _ContextCacheDetect:
    @staticmethod
    def start():
        if _var.get() is None:
            _var.set(CacheDetect())

    @staticmethod
    def set(key: str, **kwargs):
        var = _var.get()
        if var is not None:
            var.set(key, **kwargs)

    @staticmethod
    def get():
        var = _var.get()
        if var is not None:
            return var.get()
        return var

    @staticmethod
    def merge(other: CacheDetect):
        var = _var.get()
        if var is not None:
            var.merge(other)


context_cache_detect = _ContextCacheDetect()
