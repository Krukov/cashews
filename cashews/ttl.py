from datetime import timedelta
from typing import Union

from cashews._typing import TTL


def ttl_to_seconds(ttl: Union[float, None, TTL], *args, with_callable=False, **kwargs) -> Union[int, None, float]:
    if ttl is None:
        return None
    _type = type(ttl)
    if _type == int:
        return ttl
    if _type == timedelta:
        return ttl.total_seconds()
    if _type == str:
        return _ttl_from_str(ttl)
    if callable(ttl) and with_callable:
        return ttl_to_seconds(ttl(*args, **kwargs))
    return ttl


_STR_TO_DELTA = {
    "h": timedelta(hours=1),
    "m": timedelta(minutes=1),
    "s": timedelta(seconds=1),
    "d": timedelta(days=1),
}


def _ttl_from_str(ttl: str) -> int:
    result = 0
    mul = ""
    for char in ttl.strip().lower():
        if char.isdigit():
            mul += char
        elif char in _STR_TO_DELTA:
            result += int(mul) * int(_STR_TO_DELTA[char].total_seconds())
            mul = ""
        else:
            raise ValueError(f"ttl '{ttl}' has wrong string representation")
    if mul != "" and not result:
        return int(mul)
    return result
