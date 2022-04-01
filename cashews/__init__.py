from .backends.interface import LockedException  # noqa
from .decorators import (  # noqa
    NOT_NONE,
    CacheDetect,
    CircuitBreakerOpen,
    RateLimitException,
    context_cache_detect,
    fast_condition,
)
from .formatter import default_formatter, get_template_and_func_for, get_template_for_key  # noqa
from .helpers import add_prefix, all_keys_lower  # noqa
from .key import noself  # noqa
from .validation import set_invalidate_further  # noqa
from .wrapper import Cache  # noqa

cache_detect = context_cache_detect
# pylint: disable=invalid-name
cache = Cache(name="default")
noself_cache = noself(cache.cache)
rate_limit = cache.rate_limit
failover = cache.failover
circuit_breaker = cache.circuit_breaker
early = cache.early
soft = cache.soft
hit = cache.hit
dynamic = cache.dynamic
locked = cache.locked
invalidate = cache.invalidate
invalidate_func = cache.invalidate_func


mem = Cache(name="mem")
mem.setup(
    "mem://?check_interval=1",
    size=1_000_000,
)  # 1_000_000 * 248(size of small dict) == 31 mb
