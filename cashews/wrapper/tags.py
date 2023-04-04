from functools import lru_cache
from typing import Dict, Iterable, List, Match, Optional, Pattern, Tuple

from cashews._typing import TTL, Key, KeyOrTemplate, Tag, Tags, Value
from cashews.backends.interface import Backend, Callback
from cashews.exceptions import TagNotRegisteredError
from cashews.formatter import template_to_re_pattern

from .commands import CommandWrapper


class TagsRegistry:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._registry_template: Dict[Tag, List[Pattern]] = {}

    def register_tag(self, tag: Tag, key_template: KeyOrTemplate):
        self._registry_template.setdefault(tag, [])
        self._registry_template[tag].append(template_to_re_pattern(key_template))

    def get_key_tags(self, key: Key) -> Tags:
        tags = []
        for tag, patterns in self._registry_template.items():
            match = self._match_patterns(key, patterns)
            if match:
                tags.append(tag.format(**match.groupdict()))
        return tags

    @staticmethod
    def _match_patterns(key: Key, patterns: List[Pattern]) -> Optional[Match]:
        for pattern in patterns:
            match = pattern.fullmatch(key)
            if match:
                return match
        return None

    def check_tags_registered(self, key: Key, tags: Tags):
        tags = set(tags)
        key_tags = set(self.get_key_tags(key))
        difference = tags.difference(key_tags)

        if difference:
            raise TagNotRegisteredError(f"tag: {difference} not registered: call cache.register_tag before using tags")


class CommandsTagsWrapper(CommandWrapper):
    def __init__(self, name: str = ""):
        super().__init__(name)
        self._tags_registry = TagsRegistry()
        self._tags_key_prefix = "_tag:"
        self._on_remove_cb = self._on_remove_callback()

    def setup_tags_backend(self, settings_url: str, middlewares: Tuple = (), **kwargs) -> Backend:
        return self.setup(settings_url, middlewares=middlewares, prefix=self._tags_key_prefix, **kwargs)

    @lru_cache(maxsize=1)
    def _get_tags_backend(self):
        return self._get_backend(self._tags_key_prefix)

    @property
    def tags_backend(self):
        return self._get_tags_backend()

    def register_tag(self, tag: Tag, key_template: KeyOrTemplate):
        self._tags_registry.register_tag(tag, key_template)

    def _add_backend(self, backend: Backend, *args, **kwargs):
        super()._add_backend(backend, *args, **kwargs)
        backend.on_remove_callback(self._on_remove_cb)

    def _on_remove_callback(self) -> Callback:
        async def _callback(keys: Iterable[Key], **kwargs):
            for tag, _keys in self._group_by_tags(keys).items():
                await self.tags_backend.set_remove(self._tags_key_prefix + tag, *_keys)

        return _callback

    def _group_by_tags(self, keys: Iterable[Key]) -> Dict[Tag, List[Key]]:
        tags = {}
        for key in keys:
            for tag in self.get_key_tags(key):
                tags.setdefault(tag, []).append(key)
        return tags

    def get_key_tags(self, key: Key) -> Tags:
        return self._tags_registry.get_key_tags(key)

    async def delete_tags(self, *tags: Tag):
        for tag in tags:
            await self._delete_tag(tag)

    async def _delete_tag(self, tag: Tag):
        while True:
            keys = await self.set_pop(key=self._tags_key_prefix + tag, count=100)
            if not keys:
                break
            await self.delete_many(*keys)

    async def set(
        self,
        key: Key,
        value: Value,
        expire: Optional[TTL] = None,
        exist: Optional[bool] = None,
        tags: Tags = (),
    ) -> bool:
        _set = await super().set(key=key, value=value, expire=expire, exist=exist)
        if _set and tags:
            self._tags_registry.check_tags_registered(key, tags)
            for tag in tags:
                await self.set_add(self._tags_key_prefix + tag, key, expire=expire)
        return _set

    async def incr(self, key: Key, value: int = 1, expire: Optional[float] = None, tags: Tags = ()) -> int:
        _set = await super().incr(key=key, value=value, expire=expire)
        if _set and tags:
            self._tags_registry.check_tags_registered(key, tags)
            for tag in tags:
                await self.set_add(self._tags_key_prefix + tag, key, expire=expire)
        return _set
