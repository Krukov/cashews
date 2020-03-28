from typing import Any, Dict


def _default_store_condition(result) -> bool:
    return result is not None


def _default_disable_condition(args: Dict[str, Any]) -> bool:
    return False


class CacheDetect:
    def __init__(self):
        self._value = False

    def set(self):
        self._value = True

    def get(self):
        return self._value
