import random
from contextlib import AbstractContextManager
from contextvars import ContextVar
from typing import Any

from cashews._typing import Key

_empty = object()


class CacheDetect:
    __slots__ = ("_value", "_unset_token", "_previous_level")

    def __init__(self, previous_level=0, unset_token=None):
        self._value = []
        self._unset_token = unset_token
        self._previous_level = previous_level

    def _set(self, key: Key, **kwargs: Any) -> None:
        self._value.append((key, kwargs))

    @property
    def calls(self):
        return dict(self._value)

    @property
    def calls_list(self):
        return self._value.copy()

    def clear(self):
        self._value = []


class _ContextCacheDetect:
    def __init__(self) -> None:
        self._levels: dict[float, CacheDetect] = {}
        # every detector tracks its own nesting, so detectors of different caches can be nested in any order
        self._level: ContextVar[float] = ContextVar("level", default=0)

    @property
    def level(self):
        return self._level.get()

    def _get_next_level(self):
        level = random.random()
        if level not in self._levels:
            return level
        return self._get_next_level()

    def _start(self) -> CacheDetect:
        previous_level = self.level
        level = self._get_next_level()
        token = self._level.set(level)
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
            self._level.reset(token)

    def __enter__(self) -> CacheDetect:
        return self._start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop()


_context_cache_detect = _ContextCacheDetect()
context_cache_detect: AbstractContextManager[CacheDetect] = _context_cache_detect


def _set_detect(backend: Any, key: Key, **kwargs: Any) -> None:
    """
    Report a cache hit to the global detector and to the detector of the cache that served it
    """
    _context_cache_detect._set(key, **kwargs)
    detect = getattr(backend, "detect", None)
    if detect is not None and detect is not _context_cache_detect:
        detect._set(key, **kwargs)
