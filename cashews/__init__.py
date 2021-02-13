from .backends.interface import LockedException
from .decorators import (  # noqa
    CacheDetect,
    CircuitBreakerOpen,
    PerfDegradationException,
    RateLimitException,
    context_cache_detect,
    fast_condition,
)
from .helpers import add_prefix  # noqa
from .validation import set_invalidate_further  # noqa
from .wrapper import Cache  # noqa

# pylint: disable=invalid-name
cache = Cache(name="default")
rate_limit = cache.rate_limit
fail = cache.failover
failover = cache.failover
circuit_breaker = cache.circuit_breaker
early = cache.early
hit = cache.hit
dynamic = cache.dynamic
perf = cache.perf
locked = cache.locked
invalidate = cache.invalidate
invalidate_func = cache.invalidate_func


mem = Cache(name="mem")
mem.setup(
    "mem://?check_interval=1", size=1_000_000,
)  # 1_000_000 * 248(size of small dict) == 31 mb
