from ...serialize import PickleSerializerMixin
from .backend import _Redis

__all__ = "Redis"


class Redis(PickleSerializerMixin, _Redis):
    pass
