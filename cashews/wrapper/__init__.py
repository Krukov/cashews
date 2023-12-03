from cashews.backends.interface import _BackendInterface
from cashews.decorators import context_cache_detect

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
    detect = context_cache_detect
