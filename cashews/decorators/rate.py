import logging
from functools import wraps
from typing import Any, Callable, NoReturn, Optional

from cashews._typing import TTL, AsyncCallable_T, Decorator, KeyOrTemplate
from cashews.backends.interface import _BackendInterface
from cashews.exceptions import RateLimitError
from cashews.key import get_cache_key, get_cache_key_template
from cashews.ttl import ttl_to_seconds

logger = logging.getLogger(__name__)


def _default_action(*args: Any, **kwargs: Any) -> NoReturn:
    raise RateLimitError()


def rate_limit(
    backend: _BackendInterface,
    limit: int,
    period: TTL,
    ttl: Optional[TTL] = None,
    key: Optional[KeyOrTemplate] = None,
    action: Callable = _default_action,
    prefix: str = "rate_limit",
) -> Decorator:  # pylint: disable=too-many-arguments
    """
    Rate limit for function call. Do not call function if rate limit is reached, and call given action

    :param backend: cache backend
    :param limit: number of calls
    :param period: Period
    :param ttl: time ban, default == period
    :param key: a rate-limit key template
    :param action: call when rate limit reached, default raise RateLimitError
    :param prefix: custom prefix for key, default 'rate_limit'
    """
    period = ttl_to_seconds(period)
    ttl = ttl_to_seconds(ttl) or period
    action = action or _default_action

    def decorator(func: AsyncCallable_T) -> AsyncCallable_T:
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)

        @wraps(func)
        async def wrapped_func(*args, **kwargs):
            _ttl = ttl_to_seconds(ttl, *args, **kwargs, with_callable=True)
            _period = ttl_to_seconds(period, *args, **kwargs, with_callable=True)
            _cache_key = get_cache_key(func, _key_template, args, kwargs)

            requests_count = await backend.incr(key=_cache_key, expire=_period)
            if requests_count and requests_count > limit:
                if ttl and requests_count == limit + 1:
                    await backend.expire(key=_cache_key, timeout=_ttl)
                logger.info("Rate limit reach for %s", _cache_key)
                action(*args, **kwargs)
            return await func(*args, **kwargs)

        return wrapped_func

    return decorator
