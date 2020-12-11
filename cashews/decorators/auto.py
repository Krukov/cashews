import asyncio
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from typing import Callable, Optional

from ..backends.interface import Backend
from ..key import get_cache_key, get_cache_key_template
from .cache.early import early
from .cache.fail import failover


@dataclass(frozen=True)
class _Stats:
    total: int
    params: int
    errors: int
    params_errors: int


@dataclass(frozen=True)
class _Settings:
    min_calls: int
    fail_percent: int
    cache_percent: int
    errors: int


def auto(
    backend: Backend, min_calls: int = 1000, prefix: str = "auto",
):
    def decorator(func):
        @wraps(func)
        async def wrapped_func(*args, **kwargs):
            if wrapped_func._decor is not None:
                return wrapped_func._decor(func)(*args, **kwargs)
            await _get_decorator_from_cache(backend,)

            try:
                return await func(*args, **kwargs)
            except Exception:
                _key_template_error = get_cache_key_template(func, key=None, prefix=prefix + ":error")
                errors_count, all_errors_count = await asyncio.gather(
                    backend.incr(_key_template_error), backend.incr(_key_template_all_errors)
                )
                raise
            finally:
                pass

        wrapped_func._decor = None
        return wrapped_func

    return decorator


async def _get_decorator_from_cache(backend: Backend):
    pass


async def _set_cache_decor(backend: Backend, func, args, kwargs, prefix, settings: _Settings):
    _key_template = get_cache_key_template(func, key=None, prefix=prefix + ":stats")
    _cache_key_all = get_cache_key_template(func, key="all", prefix=prefix + ":stats")
    _cache_key = get_cache_key(func, _key_template, args, kwargs)

    count, total = await _get_count(backend, _cache_key, _cache_key_all)

    if total < settings.min_calls:
        return None

    if count:
        first_call_time: datetime = await backend.get(_cache_key + ":time")


async def _get_count(backend: Backend, cache_key, cache_key_all):
    count = 0
    if cache_key_all != cache_key:
        count, total = await asyncio.gather(backend.incr(cache_key), backend.incr(cache_key_all))
    else:
        total = await backend.incr(cache_key_all)
    if total == 1:
        await backend.set(cache_key_all + ":time", datetime.utcnow())

    if count == 1:
        await backend.set(cache_key + ":time", datetime.utcnow())

    return count, total


def _get_cached_decorator(
    total: int, first_call: datetime, count: int, count_call: Optional[datetime], settings: _Settings
) -> Optional[Callable]:
    """
    If local calls is more then 10% of all calls -> cache
    if errors is more then 5
    """
    if count_call:
        if float(count) / total > settings.cache_percent:
            return early(ttl=10 * _get_rate(count_call, count))
    if _get_rate(first_call, total) > settings.calls_per_sec:
        return


def _get_rate(date, count):
    return count / (datetime.utcnow() - date).total_seconds()
