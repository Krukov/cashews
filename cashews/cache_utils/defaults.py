from contextvars import ContextVar
from datetime import datetime
from typing import Any, Dict


def _default_store_condition(result) -> bool:
    return result is not None


class CacheDetect:
    def __init__(self):
        self._value = {}

    def set(self, key: str, **kwargs):
        self._value[key] = kwargs

    def get(self):
        return self._value


_var = ContextVar("cashews")
_var.set(None)


class _ContextCacheDetect:
    def start(self):
        _var.set(CacheDetect())

    def set(self, key: str, **kwargs):
        var = _var.get()
        if var is not None:
            var.set(key, **kwargs)

    def get(self):
        var = _var.get()
        if var is not None:
            return var.get()
        return var


context_cache_detect = _ContextCacheDetect()
