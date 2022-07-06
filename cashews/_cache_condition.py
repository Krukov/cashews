import time
from contextvars import ContextVar
from functools import wraps
from typing import Any, Callable, Dict, Tuple

from cashews._typing import CacheCondition


def _not_none_store_condition(result, args, kwargs, key=None) -> bool:
    return result is not None


def _store_all(result, args, kwargs, key=None) -> bool:
    return True


NOT_NONE = "not_none"
_ALL_CONDITIONS = {Any, "all", any, "any", None}
_NOT_NONE_CONDITIONS = {NOT_NONE, "skip_none"}


def get_cache_condition(condition: CacheCondition) -> Callable[[Any, Tuple, Dict], bool]:
    if condition in _ALL_CONDITIONS:
        return _store_all
    if condition in _NOT_NONE_CONDITIONS:
        return _not_none_store_condition
    return condition


_spent = ContextVar("spent", default=0)


def create_time_condition(limit):
    def decorator(func):
        @wraps(func)
        async def _wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return await func(*args, **kwargs)
            finally:
                _spent.set(time.perf_counter() - start)

        return _wrapper

    def condition(result, _args, _kwargs, key):
        return _spent.get() > limit

    return condition, decorator
