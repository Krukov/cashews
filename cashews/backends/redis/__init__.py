from cashews.picklers import DEFAULT_PICKLE
from cashews.serialize import SerializerMixin

from .backend import _Redis

__all__ = ["Redis"]


class Redis(SerializerMixin, _Redis):
    pickle_type = DEFAULT_PICKLE
