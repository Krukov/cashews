from __future__ import annotations

import time
from contextvars import ContextVar
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:  # pragma: no cover
    from cashews._typing import CallableCacheCondition, DecoratedFunc

_spent = ContextVar("spent", default=0.0)


def create_time_condition(
    limit: float,
) -> tuple[CallableCacheCondition, Callable[[DecoratedFunc], DecoratedFunc]]:
    def decorator(func: DecoratedFunc) -> DecoratedFunc:
        @wraps(func)
        async def _wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return await func(*args, **kwargs)
            finally:
                _spent.set(time.perf_counter() - start)

        return _wrapper  # type: ignore[return-value]

    def condition(result: Any, args: tuple, kwargs: dict[str, Any], key: str = "") -> bool:
        return _spent.get() > limit

    return condition, decorator
