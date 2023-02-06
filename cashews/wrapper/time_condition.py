import time
from contextvars import ContextVar
from functools import wraps
from typing import Any, Dict, Tuple

from cashews._typing import AsyncCallable_T, CallableCacheCondition, Decorator

_spent = ContextVar("spent", default=0.0)


def create_time_condition(limit: float) -> Tuple[CallableCacheCondition, Decorator]:
    def decorator(func: AsyncCallable_T) -> AsyncCallable_T:
        @wraps(func)
        async def _wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return await func(*args, **kwargs)
            finally:
                _spent.set(time.perf_counter() - start)

        return _wrapper

    def condition(result: Any, args: Tuple, kwargs: Dict[str, Any], key: str = "") -> bool:
        return _spent.get() > limit

    return condition, decorator
