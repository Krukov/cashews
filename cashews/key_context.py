from __future__ import annotations

import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

_REWRITE = "__rewrite"
TContext = dict[str, Any]
_template_context: ContextVar[TContext | None] = ContextVar("template_context", default=None)
_empty = object()


@contextmanager
def context(rewrite=_empty, **values) -> Iterator[None]:
    if rewrite is not _empty:
        warnings.warn(
            "`rewrite` deprecated and will be removed in next release, use @ notation",
            DeprecationWarning,
            stacklevel=2,
        )
    else:
        rewrite = False
    new_context = {**_get_raw(), **values, _REWRITE: rewrite}
    token = _template_context.set(new_context)
    try:
        yield
    finally:
        _template_context.reset(token)


def get() -> tuple[TContext, bool]:
    _context = {**_get_raw()}  # a copy
    return _context, _context.pop(_REWRITE)


def _get_raw() -> TContext:
    return _template_context.get() or {_REWRITE: False}


def register(*names: str) -> None:
    warnings.warn(
        "`register_key_context` deprecated and will be removed in next release, use @ notation",
        DeprecationWarning,
        stacklevel=2,
    )
    new_names = dict.fromkeys(names, "")
    _template_context.set({**new_names, **_get_raw()})
