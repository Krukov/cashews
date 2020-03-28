from .backends.interface import LockedException
from .cache_utils import CacheDetect, CircuitBreakerOpen, RateLimitException  # noqa
from .wrapper import Cache, add_prefix  # noqa

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
