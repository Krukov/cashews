from .backends.interface import LockedException
from .cache_utils import (  # noqa
    CacheDetect,
    CircuitBreakerOpen,
    PerfDegradationException,
    RateLimitException,
    context_cache_detect,
)
from .helpers import add_prefix, at  # noqa
from .wrapper import Cache  # noqa

# pylint: disable=invalid-name
cache = Cache()
rate_limit = cache.rate_limit
fail = cache.fail
circuit_breaker = cache.circuit_breaker
early = cache.early
hit = cache.hit
perf = cache.perf
locked = cache.locked
invalidate = cache.invalidate


mem = Cache()
mem.setup(
    "mem://?check_interval=1", size=1_000_000, middlewares=(add_prefix("mem:"),),
)  # 1_000_000 * 248(size of small dict) == 31 mb
