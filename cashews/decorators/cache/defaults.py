import random
from contextvars import ContextVar
from typing import Any, Dict

from ..._typing import CacheCondition

_empty = object()


def _not_none_store_condition(result, args, kwargs, key=None) -> bool:
    return result is not None


def _store_all(result, args, kwargs, key=None) -> bool:
    return True


NOT_NONE = "not_none"
_ALL_CONDITIONS = {Any, "all", any, "any", None}
_NOT_NONE_CONDITIONS = {NOT_NONE, "skip_none"}


def _get_cache_condition(condition: CacheCondition):
    if condition in _ALL_CONDITIONS:
        return _store_all
    if condition in _NOT_NONE_CONDITIONS:
        return _not_none_store_condition
    return condition


class CacheDetect:
    __slots__ = ("_value", "_unset_token", "_previous_level")

    def __init__(self, previous_level=0, unset_token=None):
        self._value = {}
        self._unset_token = unset_token
        self._previous_level = previous_level

    def _set(self, key: str, **kwargs):
        self._value.setdefault(key, []).append(kwargs)

    @property
    def calls(self):
        return dict(self._value)

    keys = calls  # backward compatibility

    def clear(self):
        self._value = {}


_level = ContextVar("level", default=0)


class _ContextCacheDetect:
    def __init__(self):
        self._levels: Dict[int, CacheDetect] = {}

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
            var = self._levels.get(level)
            if var is None:
                return
            var._set(key, **kwargs)
            level = var._previous_level

    def _stop(self):
        if self.level in self._levels:
            token = self._levels[self.level]._unset_token
            self._levels[self.level].clear()
            del self._levels[self.level]
            _level.reset(token)

    def __enter__(self) -> CacheDetect:
        return self._start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop()


context_cache_detect = _ContextCacheDetect()
