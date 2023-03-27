from typing import Any, Callable, Dict, Tuple, Type, Union
from urllib.parse import parse_qsl, urlparse

from cashews.backends.interface import Backend
from cashews.backends.memory import Memory
from cashews.exceptions import BackendNotAvailableError

try:
    import redis  # noqa: F401
except ImportError:
    BcastClientSide, Redis = None, None
else:
    from cashews.backends.redis import Redis
    from cashews.backends.redis.client_side import BcastClientSide

try:
    import diskcache  # noqa: F401
except ImportError:
    DiskCache = None
else:
    from cashews.backends.diskcache import DiskCache


_NO_REDIS_ERROR = "Redis backend requires `redis` to be installed."
_CUSTOM_ERRORS = {
    "redis": _NO_REDIS_ERROR,
    "rediss": _NO_REDIS_ERROR,
    "disk": "Disk backend requires `diskcache` to be installed.",
}
BackendOrFabric = Union[Type[Backend], Callable[..., Backend]]
_BACKENDS: Dict[str, Tuple[BackendOrFabric, bool]] = {}


def register_backend(alias: str, backend_class: BackendOrFabric, pass_uri: bool = False):
    _BACKENDS[alias] = (backend_class, pass_uri)


def _redis_fabric(**params: Any) -> Union[Redis, BcastClientSide]:
    if params.pop("client_side", None):
        return BcastClientSide(**params)
    return Redis(**params)


register_backend("mem", Memory)
if Redis:
    register_backend("redis", _redis_fabric, pass_uri=True)
    register_backend("rediss", _redis_fabric, pass_uri=True)
if DiskCache:
    register_backend("disk", DiskCache)


def settings_url_parse(url: str) -> Tuple[BackendOrFabric, Dict[str, Any]]:
    parse_result = urlparse(url)
    params: Dict[str, Any] = dict(parse_qsl(parse_result.query))
    params = serialize_params(params)

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


def serialize_params(params: Dict[str, str]) -> Dict[str, Union[str, int, bool, float]]:
    new_params = {}
    bool_keys = ("safe", "suppress", "enable", "disable", "client_side")
    true_values = (
        "1",
        "true",
    )
    for key, value in params.items():
        _value: Union[str, int, bool, float]
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
