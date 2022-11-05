from datetime import timedelta
from typing import Any, Callable, Dict, Optional, Tuple, Union

_TTLTypes = Union[int, float, str, timedelta, None]
TTL = Union[_TTLTypes, Callable[[Any], _TTLTypes]]
CallableCacheCondition = Callable[[Any, Tuple, Dict, Optional[str]], bool]
CacheCondition = Union[CallableCacheCondition, str, None]
