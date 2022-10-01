class CacheError(Exception):
    pass


class UnsupportedPicklerError(CacheError):
    """Unknown or unsupported pickle type."""


class UnSecureDataError(CacheError):
    pass


class SignIsMissingError(CacheError):
    ...


class WrongKeyError(CacheError):
    """Raised If key template have wrong parameter."""


class LockedError(CacheError):
    pass


class CacheBackendInteractionError(CacheError):
    pass


class RateLimitError(CacheError):
    pass
