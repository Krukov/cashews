from typing import Any, Dict


def _default_store_condition(result) -> bool:
    return result is not None


def _default_disable_condition(args: Dict[str, Any]) -> bool:
    return False
