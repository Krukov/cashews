from datetime import timedelta
from typing import Any, Callable, Dict, Optional, Tuple, Union

FuncArgsType = Optional[Union[Dict[str, Callable[[Any], str]], Tuple[str]]]
TTL = Union[int, timedelta, Callable[[], Union[int, timedelta]]]
CacheCondition = Union[Callable[[Any, Tuple, Dict], bool], str, None]
