class CacheError(Exception):
    pass


class BackendNotAvailableError(CacheError):
    """For wrong or not available cache alias"""


class NotConfiguredError(CacheError):
    """If cache was not configured"""


class UnsupportedPicklerError(CacheError):
    """Unknown or unsupported pickle type."""


class UnSecureDataError(CacheError):
    """Unsecure data in cache storage"""


class SignIsMissingError(CacheError):
    ...


class WrongKeyError(CacheError):
    """Raised If key template have wrong parameter."""


class TagNotRegisteredError(CacheError):
    """Raised If tag not registered."""


class LockedError(CacheError):
    """Raised if a key already locked"""


class CacheBackendInteractionError(CacheError):
    """Raised if redis not available and safe is set to false"""


class RateLimitError(CacheError):
    """Raised by @rate_limit if rate limit is reached"""


class CircuitBreakerOpen(Exception):
    """Raised by @circuit_breaker"""
