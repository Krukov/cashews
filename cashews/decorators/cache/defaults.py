import random
from contextvars import ContextVar
from typing import Any, Dict

from cashews._typing import Key

_empty = object()


class CacheDetect:
    __slots__ = ("_value", "_unset_token", "_previous_level")

    def __init__(self, previous_level=0, unset_token=None):
        self._value = []
        self._unset_token = unset_token
        self._previous_level = previous_level

    def _set(self, key: Key, **kwargs: Any) -> None:
        self._value.append((key, [kwargs]))

    @property
    def calls(self):
        return dict(self._value)

    @property
    def calls_list(self):
        return self._value.copy()

    def clear(self):
        self._value = []


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

    def _set(self, key: str, **kwargs: Any) -> None:
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
