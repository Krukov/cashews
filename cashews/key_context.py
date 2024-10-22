from __future__ import annotations

import warnings
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

_REWRITE = "__rewrite"
_template_context: ContextVar[dict[str, Any]] = ContextVar("template_context", default={_REWRITE: False})
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
    new_context = {**_template_context.get(), **values}
    new_context[_REWRITE] = rewrite
    token = _template_context.set(new_context)
    try:
        yield
    finally:
        _template_context.reset(token)


def get() -> tuple[dict[str, Any], bool]:
    _context = {**_template_context.get()}
    return _context, _context.pop(_REWRITE)


def register(*names: str) -> None:
    warnings.warn(
        "`register_key_context` deprecated and will be removed in next release, use @ notation",
        DeprecationWarning,
        stacklevel=2,
    )
    new_names = {name: "" for name in names}
    _template_context.set({**new_names, **_template_context.get()})
