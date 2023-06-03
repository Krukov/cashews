from functools import partial
from typing import Any, Callable, Dict, Tuple

from ._typing import CacheCondition


def _not_none_store_condition(result, args, kwargs, key=None) -> bool:
    return result is not None


def _store_all(result, args, kwargs, key=None) -> bool:
    return True


def _exceptions(*exceptions: Exception, default: bool = True) -> CacheCondition:
    exceptions = exceptions or Exception

    def _cond(result, args, kwargs, key):
        if isinstance(result, exceptions):
            return result
        return default

    return _cond


with_exceptions = partial(_exceptions, default=True)
only_exceptions = partial(_exceptions, default=False)


NOT_NONE = "not_none"
_ALL_CONDITIONS = {Any, "all", any, "any", None}
_NOT_NONE_CONDITIONS = {NOT_NONE, "skip_none"}


def get_cache_condition(condition: CacheCondition) -> Callable[[Any, Tuple, Dict], bool]:
    if condition in _ALL_CONDITIONS:
        return _store_all
    if condition in _NOT_NONE_CONDITIONS:
        return _not_none_store_condition
    return condition
