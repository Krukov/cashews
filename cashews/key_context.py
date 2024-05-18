from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

_REWRITE = "__rewrite"
_template_context: ContextVar[dict[str, Any]] = ContextVar("template_context", default={_REWRITE: False})


@contextmanager
def context(rewrite=False, **values) -> Iterator[None]:
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
    new_names = {name: "" for name in names}
    _template_context.set({**new_names, **_template_context.get()})
