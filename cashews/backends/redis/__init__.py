from ...serialize import PickleSerializerMixin
from .backend import _Redis
from .compat import AIOREDIS_IS_VERSION_1

__all__ = ("Redis",)


class Redis(PickleSerializerMixin, _Redis):
    pass
