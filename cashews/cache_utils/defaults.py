from contextvars import ContextVar

_empty = object()


def _default_store_condition(result, args, kwargs) -> bool:
    return result is not None


class CacheDetect:
    def __init__(self):
        self._value = {}

    def set(self, key: str, **kwargs):
        self._value[key] = kwargs

    def get(self):
        return self._value


_var = ContextVar("cashews")
_var.set(None)


class _ContextCacheDetect:
    @staticmethod
    def start():
        _var.set(CacheDetect())

    @staticmethod
    def set(key: str, **kwargs):
        var = _var.get()
        if var is not None:
            var.set(key, **kwargs)

    @staticmethod
    def get():
        var = _var.get()
        if var is not None:
            return var.get()
        return var


context_cache_detect = _ContextCacheDetect()
