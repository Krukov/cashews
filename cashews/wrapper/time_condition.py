from __future__ import annotations

import time
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:  # pragma: no cover
    from cashews._typing import CallableCacheCondition, DecoratedFunc

_NAME = "_spend"


def create_time_condition(
    limit: float, backend
) -> tuple[CallableCacheCondition, Callable[[DecoratedFunc], DecoratedFunc]]:
    def decorator(func: DecoratedFunc) -> DecoratedFunc:
        @wraps(func)
        async def _wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return await func(*args, **kwargs)
            finally:
                backend._set_cache_context(name=_NAME, value=time.perf_counter() - start)

        return _wrapper  # type: ignore[return-value]

    def condition(result: Any, args: tuple, kwargs: dict[str, Any], key: str = "") -> bool:
        return backend._get_cache_context_value(name=_NAME) > limit

    return condition, decorator
