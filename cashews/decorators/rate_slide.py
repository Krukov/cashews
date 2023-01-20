import logging
from datetime import datetime
from functools import wraps
from typing import Any, Callable, NoReturn, Optional

from .._typing import TTL, AsyncCallable_T, Decorator
from ..backends.interface import _BackendInterface
from ..exceptions import RateLimitError
from ..formatter import register_template
from ..key import get_cache_key, get_cache_key_template
from ..ttl import ttl_to_seconds

logger = logging.getLogger(__name__)


def _default_action(*args: Any, **kwargs: Any) -> NoReturn:
    raise RateLimitError()


def slice_rate_limit(
    backend: _BackendInterface,
    limit: int,
    period: TTL,
    key: Optional[str] = None,
    action: Callable = _default_action,
    prefix: str = "srate",
) -> Decorator:  # pylint: disable=too-many-arguments
    """
    Rate limit for function call. Do not call function if rate limit is reached, and call given action

    :param backend: cache backend
    :param limit: number of calls
    :param period: Period
    :param key: a rate-limit key template
    :param action: call when rate limit reached, default raise RateLimitError
    :param prefix: custom prefix for key, default 'rate_limit'
    """
    period = ttl_to_seconds(period)
    action = action or _default_action

    def decorator(func: AsyncCallable_T) -> AsyncCallable_T:
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)
        register_template(func, _key_template)

        @wraps(func)
        async def wrapped_func(*args, **kwargs):
            _period = ttl_to_seconds(period, *args, **kwargs, with_callable=True)
            _cache_key = get_cache_key(func, _key_template, args, kwargs)

            requests_count = await _get_requests_count(backend, _cache_key, limit, _period)
            if requests_count and requests_count > limit:
                logger.info("Rate limit reach for %s", _cache_key)
                action(*args, **kwargs)
            return await func(*args, **kwargs)

        return wrapped_func

    return decorator


async def _get_requests_count(backend: _BackendInterface, key: str, limit: int, period: int) -> int:
    timestamp = datetime.utcnow().timestamp()
    return await backend.slice_incr(key, timestamp - period, timestamp, maxvalue=limit + 1, expire=period)