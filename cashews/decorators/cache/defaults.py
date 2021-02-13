import random
from contextvars import ContextVar
from typing import Any

from ...typing import CacheCondition

_empty = object()


def _default_store_condition(result, args, kwargs, key=None) -> bool:
    return result is not None


def _store_all(result, args, kwargs, key=None) -> bool:
    return True


def _get_cache_condition(condition: CacheCondition):
    if condition is None:
        return _default_store_condition
    if condition in [Any, "all", any, "any"]:
        return _store_all
    return condition


class CacheDetect:
    def __init__(self, previous_level=0, unset_token=None):
        self._value = {}
        self._unset_token = unset_token
        self._previous_level = previous_level

    def _set(self, key: str, **kwargs):
        self._value.setdefault(key, []).append(kwargs)

    @property
    def keys(self):
        return dict(self._value)


_level = ContextVar("level", default=0)


class _ContextCacheDetect:
    def __init__(self):
        self._levels = {}

    @property
    def level(self):
        return _level.get()

    def _get_next_level(self):
        level = random.random()
        if level not in self._levels:
            return level
        return self._get_next_level()

    def _start(self) -> CacheDetect:
        previous_level = self.level
        level = self._get_next_level()
        token = _level.set(level)
        self._levels[level] = CacheDetect(previous_level=previous_level, unset_token=token)
        return self._levels[level]

    def _set(self, key: str, **kwargs):
        level = self.level
        while level:
            var: CacheDetect = self._levels.get(level)
            if var is None:
                return
            var._set(key, **kwargs)
            level = var._previous_level

    def _stop(self):
        if self.level in self._levels:
            token = self._levels[self.level]._unset_token
            del self._levels[self.level]
            _level.reset(token)

    def __enter__(self) -> CacheDetect:
        return self._start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop()


context_cache_detect = _ContextCacheDetect()
