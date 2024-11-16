from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, NamedTuple

from cashews._typing import TTL, Key, Value

from .commands import CommandWrapper

_Name = str
_CONTEXT_DEFAULT_PREFIX = "_context:"
_TEMPLATE_CONTEXT = "_template"


class CacheContext(NamedTuple):
    name: _Name
    value: Any
    key: bool
    ttl: TTL | None


class ContextualWrapper(CommandWrapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._context: ContextVar[dict[_Name, CacheContext]] = ContextVar(f"Context_{id(self)}", default={})
        self._context_prefix = _CONTEXT_DEFAULT_PREFIX

    def _get_cache_context_value(self, name: _Name) -> Any | None:
        cache_value = self._context.get().get(name)
        if cache_value is not None:
            return cache_value.value
        return None

    def _set_cache_context(self, name: _Name, value: Any, key: bool = False, ttl: TTL | None = None):
        current_context = self._context.get()
        self._context.set({**current_context, name: CacheContext(name=name, value=value, key=key, ttl=ttl)})

    @contextmanager
    def cache_context(self, key: bool = False, **values):
        current_context = self._context.get()
        token = self._context.set(
            {
                **current_context,
                **{name: CacheContext(name=name, value=value, key=key, ttl=None) for name, value in values.items()},
            }
        )
        try:
            yield
        finally:
            self._context.reset(token)

    @contextmanager
    def template_context(self, **values):
        _template_context = self._get_cache_context_value(_TEMPLATE_CONTEXT) or {}
        with self.cache_context(key=False, **{_TEMPLATE_CONTEXT: {**values, **_template_context}}):
            yield

    async def set_with_context(
        self,
        key: Key,
        value: Value,
        expire: TTL,
    ):
        context_pairs = {}
        context_ttl = None
        for value in self._context.get().values():
            if value.key:
                context_ttl = max(context_ttl, value.ttl)
                context_pairs[value.name] = value.value
        if context_pairs:
            await self.set(key=f"{self._context_prefix}{key}", value=context_pairs, expire=context_ttl)
        return await self.set(key=key, value=value, expire=expire)

    async def get_with_conntext(self, key: Key):
        main_value = self.get(key)
        if main_value is None:
            return main_value
        context = await self.get(f"{self._context_prefix}{key}")
        context_values = {}
        if context:
            for name, value in context.items():
                context_values[name] = CacheContext(name=name, value=value, key=True, ttl=None)
        return main_value, context_values
