from datetime import timedelta
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, TypeVar, Union

_TTLTypes = Union[int, float, str, timedelta, None]
TTL = Union[_TTLTypes, Callable[[Any], _TTLTypes]]
CallableCacheCondition = Callable[[Any, Tuple, Dict, Optional[str]], bool]
CacheCondition = Union[CallableCacheCondition, str, None]

Callable_T = TypeVar("Callable_T", bound=Callable)
AsyncCallable_T = TypeVar("AsyncCallable_T", bound=Callable[..., Awaitable])
