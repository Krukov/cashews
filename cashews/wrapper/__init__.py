from __future__ import annotations

from contextlib import AbstractContextManager

from cashews.backends.interface import _BackendInterface
from cashews.decorators import CacheDetect, context_cache_detect
from cashews.decorators.cache.defaults import _ContextCacheDetect

from .backend_settings import register_backend  # noqa
from .callback import CallbackWrapper
from .decorators import DecoratorsWrapper
from .disable_control import ControlWrapper
from .tags import CommandsTagsWrapper
from .transaction import TransactionMode, TransactionWrapper

__all__ = [
    "Cache",
    "TransactionMode",
    "register_backend",
]


class Cache(
    TransactionWrapper,
    ControlWrapper,
    CallbackWrapper,
    CommandsTagsWrapper,
    DecoratorsWrapper,
    _BackendInterface,
):
    detect: AbstractContextManager[CacheDetect] = context_cache_detect

    def __init__(self, name: str = "") -> None:
        super().__init__(name)
        # reports only the cache hits of this instance
        self.detect = _ContextCacheDetect()
