from typing import Dict, Union
from urllib.parse import parse_qsl, urlparse

from .backends.memory import Memory

try:
    from .backends.client_side import BcastClientSide
    from .backends.redis import Redis
except ImportError:
    BcastClientSide, Redis = None, None

try:
    import diskcache
except ImportError:
    DiskCache = None
else:
    from .backends.diskcache import DiskCache


class BackendNotAvailable(Exception):
    pass


def settings_url_parse(url):
    params = {}
    parse_result = urlparse(url)
    params.update(dict(parse_qsl(parse_result.query)))
    params = _fix_params_types(params)
    if parse_result.scheme == "redis" or parse_result.scheme == "rediss":
        if Redis is None:
            raise BackendNotAvailable("Redis backend requires `redis` (or `aioredis`) to be installed.")
        params["backend"] = Redis
        params["address"] = parse_result._replace(query=None)._replace(fragment=None).geturl()
    elif parse_result.scheme == "mem":
        params["backend"] = Memory
    elif parse_result.scheme == "disk":
        if DiskCache is None:
            raise BackendNotAvailable("Disk backend requires `diskcache` to be installed.")
        params["backend"] = DiskCache
    elif parse_result.scheme == "":
        params["backend"] = Memory
        params["disable"] = True
    else:
        raise BackendNotAvailable(f"wrong backend alias {parse_result.scheme}")
    return params


def _fix_params_types(params: Dict[str, str]) -> Dict[str, Union[str, int, bool, float]]:
    new_params = {}
    bool_keys = ("safe", "enable", "disable", "client_side")
    true_values = (
        "1",
        "true",
    )
    for key, value in params.items():
        if key.lower() in bool_keys:
            value = value.lower() in true_values
        elif value.isdigit():
            value = int(value)
        else:
            try:
                value = float(value)
            except ValueError:
                pass
        new_params[key.lower()] = value
    return new_params
