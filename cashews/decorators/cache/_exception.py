class RaiseException:
    def __init__(self, exc: Exception):
        self.exc = exc


def return_or_raise(result):
    if isinstance(result, RaiseException):
        raise result.exc
    return result
