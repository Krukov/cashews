from ...serialize import PickleSerializerMixin
from .backend import _Redis, RedisConnectionError

__all__ = ("Redis", "RedisConnectionError")


class Redis(PickleSerializerMixin, _Redis):
    pass
