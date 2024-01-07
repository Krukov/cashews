from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

_template_context: ContextVar[dict[str, Any]] = ContextVar("template_context", default={})


@contextmanager
def context(**values) -> Iterator[None]:
    new_context = {**_template_context.get(), **values}
    token = _template_context.set(new_context)
    try:
        yield
    finally:
        _template_context.reset(token)


def get():
    return {**_template_context.get()}


def register(*names: str) -> None:
    new_names = {name: "" for name in names}
    _template_context.set({**new_names, **_template_context.get()})
