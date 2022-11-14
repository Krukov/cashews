import logging
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


def rate_limit(
    backend: _BackendInterface,
    limit: int,
    period: TTL,
    ttl: Optional[TTL] = None,
    key: Optional[str] = None,
    action: Optional[Callable] = None,
    prefix: str = "rate_limit",
) -> Decorator:  # pylint: disable=too-many-arguments
    """
    Rate limit for function call. Do not call function if rate limit is reached, and call given action

    :param backend: cache backend
    :param limit: number of calls
    :param period: Period
    :param ttl: time ban, default == period
    :param key: a cache key template
    :param action: call when rate limit reached, default raise RateLimitError
    :param prefix: custom prefix for key, default 'rate_limit'
    """
    action = _default_action if action is None else action

    def decorator(func: AsyncCallable_T) -> AsyncCallable_T:
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)
        register_template(func, _key_template)

        @wraps(func)
        async def wrapped_func(*args, **kwargs):
            _ttl = ttl_to_seconds(ttl, *args, **kwargs)
            _period = ttl_to_seconds(period, *args, **kwargs)
            _cache_key = get_cache_key(func, _key_template, args, kwargs)

            requests_count = await backend.incr(key=_cache_key)  # set 1 if not exists
            if requests_count and requests_count > limit:
                if ttl and requests_count == limit + 1:
                    await backend.expire(key=_cache_key, timeout=_ttl)
                logger.info("Rate limit reach for %s", _cache_key)
                action(*args, **kwargs)

            if requests_count == 1:
                await backend.expire(key=_cache_key, timeout=_period)

            return await func(*args, **kwargs)

        return wrapped_func

    return decorator
