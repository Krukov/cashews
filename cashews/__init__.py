from .cache_utils.locked import LockedException  # noqa
from .cache_utils.rate import RateLimitException  # noqa
from .wrapper import Cache

# pylint: disable=invalid-name
cache = Cache()
rate_limit = cache.rate_limit
fail = cache.fail
early = cache.early
hit = cache.hit
perf = cache.perf
locked = cache.locked
invalidate = cache.invalidate
