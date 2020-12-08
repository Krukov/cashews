import uuid
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
        self._previous_level = _previous_level.get()

    def set(self, key: str, **kwargs):
        self._value.setdefault(key, []).append(kwargs)

    def get(self):
        return dict(self._value)

    def merge(self, other):
        self._value.update(other._value)


_previous_level = ContextVar("_previous_level", default=None)
_level = ContextVar("level", default=uuid.uuid4())


class _ContextCacheDetect:
    def __init__(self):
        self._levels = {}

    @property
    def level(self):
        return _level.get()

    def start(self):
        _previous_level.set(self.level)
        level = uuid.uuid4()
        _level.set(level)
        self._levels[level] = CacheDetect()
        return self._levels[level]

    def set(self, key: str, **kwargs):
        level = self.level
        while level:
            var = self._levels.get(level)
            if var is None:
                return
            var.set(key, **kwargs)
            level = var._previous_level

    def get(self):
        var = self._levels.get(self.level)
        if var is not None:
            return var.get()

    @property
    def calls(self):
        return self.get()

    def merge(self, other: CacheDetect):
        var = self._levels.get(self.level)
        if var is not None:
            var.merge(other)

    def stop(self):
        if self.level in self._levels:
            del self._levels[self.level]
        _level.set(_previous_level.get())

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


context_cache_detect = _ContextCacheDetect()
