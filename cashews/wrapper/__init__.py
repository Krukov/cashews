from cashews.backends.interface import _BackendInterface
from cashews.decorators import context_cache_detect

from .backend_settings import register_backend  # noqa
from .commands import CommandWrapperMixin
from .decorators import DecoratorsWrapperMixin
from .disable_control import ControlWrapperMixin
from .wrapper import Wrapper

__all__ = ["Cache", "register_backend"]


class Cache(CommandWrapperMixin, ControlWrapperMixin, DecoratorsWrapperMixin, Wrapper, _BackendInterface):
    detect = context_cache_detect
