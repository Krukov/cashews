from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Union
from urllib.parse import parse_qsl, urlparse

from cashews.backends.interface import Backend
from cashews.backends.memory import Memory
from cashews.exceptions import BackendNotAvailableError

if TYPE_CHECKING:  # pragma: no cover
    BackendOrFabric = Union[type[Backend], Callable[..., Backend]]

_NO_REDIS_ERROR = "Redis backend requires `redis` to be installed."
_CUSTOM_ERRORS = {
    "redis": _NO_REDIS_ERROR,
    "rediss": _NO_REDIS_ERROR,
    "disk": "Disk backend requires `diskcache` to be installed.",
}
_BACKENDS: dict[str, tuple[BackendOrFabric, bool]] = {}


def register_backend(alias: str, backend_class: BackendOrFabric, pass_uri: bool = False) -> None:
    _BACKENDS[alias] = (backend_class, pass_uri)


register_backend("mem", Memory)


try:
    import redis  # noqa: F401
except ImportError:
    pass
else:
    from cashews.backends.redis import Redis
    from cashews.backends.redis.client_side import BcastClientSide

    def _redis_fabric(**params) -> Redis | BcastClientSide:
        if params.pop("client_side", None):
            return BcastClientSide(**params)
        return Redis(**params)

    register_backend("redis", _redis_fabric, pass_uri=True)
    register_backend("rediss", _redis_fabric, pass_uri=True)


try:
    import diskcache  # noqa: F401
except ImportError:
    pass
else:
    from cashews.backends.diskcache import DiskCache

    register_backend("disk", DiskCache)


def settings_url_parse(url: str) -> tuple[BackendOrFabric, dict[str, Any]]:
    parse_result = urlparse(url)
    params: dict[str, Any] = dict(parse_qsl(parse_result.query))
    params = _serialize_params(params)

    alias = parse_result.scheme
    if alias == "":
        return Memory, {"disable": True}

    if alias not in _BACKENDS:
        error = _CUSTOM_ERRORS.get(alias, f"wrong backend alias {alias}")
        raise BackendNotAvailableError(error)
    backend_class, pass_uri = _BACKENDS[alias]
    if pass_uri:
        params["address"] = url.split("?")[0]
    return backend_class, params


def _serialize_params(params: dict[str, str]) -> dict[str, str | int | bool | float]:
    new_params = {}
    bool_keys = ("safe", "suppress", "enable", "disable", "client_side")
    true_values = (
        "1",
        "true",
    )
    for key, value in params.items():
        _value: str | int | bool | float
        if key.lower() in bool_keys:
            _value = value.lower() in true_values
        elif value.isdigit():
            _value = int(value)
        else:
            try:
                _value = float(value)
            except ValueError:
                _value = value
        new_params[key.lower()] = _value
    return new_params
