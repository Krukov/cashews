from .cache_condition import NOT_NONE, only_exceptions, with_exceptions
from .commands import Command
from .contrib import *  # noqa
from .decorators import context_cache_detect, fast_condition, thunder_protection
from .exceptions import CacheBackendInteractionError, CircuitBreakerOpen, LockedError, RateLimitError
from .formatter import default_formatter, get_template_and_func_for, get_template_for_key
from .helpers import add_prefix, all_keys_lower, memory_limit
from .key import get_cache_key_template, noself
from .validation import invalidate_further
from .wrapper import Cache, TransactionMode, register_backend

# pylint: disable=invalid-name
cache = Cache(name="default")
noself_cache = noself(cache.cache)
failover = cache.failover
early = cache.early
soft = cache.soft
hit = cache.hit
transaction = cache.transaction
setup = cache.setup
cache_detect = cache.detect

circuit_breaker = cache.circuit_breaker
dynamic = cache.dynamic
rate_limit = cache.rate_limit
slice_rate_limit = cache.slice_rate_limit
locked = cache.locked

invalidate = cache.invalidate
invalidate_func = cache.invalidate_func


mem = Cache(name="mem")
mem.setup(
    "mem://?check_interval=1",
    size=1_000_000,
)  # 1_000_000 * 248(small dict size) == 31 mb


__all__ = [
    "NOT_NONE",
    "only_exceptions",
    "with_exceptions",
    "Command",
    "context_cache_detect",
    "fast_condition",
    "thunder_protection",
    "CacheBackendInteractionError",
    "CircuitBreakerOpen",
    "LockedError",
    "RateLimitError",
    "default_formatter",
    "get_template_and_func_for",
    "get_template_for_key",
    "add_prefix",
    "all_keys_lower",
    "memory_limit",
    "get_cache_key_template",
    "noself",
    "invalidate_further",
    "Cache",
    "TransactionMode",
    "register_backend",
]
