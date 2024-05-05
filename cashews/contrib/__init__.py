"""
Here modules with auto setup
"""

from contextlib import suppress

with suppress(ImportError):
    from . import _starlette  # noqa
