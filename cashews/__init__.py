from .backends.interface import LockedException
from .decorators import (  # noqa
    CacheDetect,
    CircuitBreakerOpen,
    PerfDegradationException,
    RateLimitException,
    context_cache_detect,
)
from .helpers import add_prefix, at  # noqa
from .validation import set_invalidate_further  # noqa
from .wrapper import Cache  # noqa

# pylint: disable=invalid-name
cache = Cache()
rate_limit = cache.rate_limit
fail = cache.fail
circuit_breaker = cache.circuit_breaker
early = cache.early
hit = cache.hit
dynamic = cache.dynamic
perf = cache.perf
locked = cache.locked
invalidate = cache.invalidate
invalidate_func = cache.invalidate_func


mem = Cache()
mem.setup(
    "mem://?check_interval=1", size=1_000_000, middlewares=(add_prefix("mem:"),),
)  # 1_000_000 * 248(size of small dict) == 31 mb
