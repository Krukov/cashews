from datetime import timedelta
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, Union, TypeVar


TTL = Union[int, str, timedelta, Callable[[], Union[int, timedelta]]]
CallableCacheCondition = Callable[[Any, Tuple, Dict, Optional[str]], bool]
CacheCondition = Union[CallableCacheCondition, str, None]

Callable_T = TypeVar("Callable_T", bound=Callable)
AsyncCallable_T = TypeVar("AsyncCallable_T", bound=Callable[..., Awaitable])
