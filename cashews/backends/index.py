from typing import Tuple

from cashews.formatter import get_template_for_key

from .redis import Redis


class IndexRedis(Redis):
    """
    Backend based on redis backend to have ability to invalidate the group of keys based on index

    for example index = user_uid
    hkey user:{user_uid} {"info": ..., "wallets": ...., "whatever": .....}

    accounts:{user_uid} -> index_name:{user_uid} {"accounts": ....}
    """

    def __init__(self, index_name, index_field, *args, **kwargs):
        self._index_name = index_name
        self._index_field = index_field
        super().__init__(*args, **kwargs)

    def _get_index_and_key(self, key) -> Tuple[str, str]:
        template, groups = get_template_for_key(key)
        if not template or self._index_field not in groups:
            return "", key
        key = key.replace(":" + groups[self._index_field], "").replace(groups[self._index_field], "")
        return self._index_name + ":" + groups[self._index_field], key

    def set(self, key: str, value, *args, **kwargs):
        index, key = self._get_index_and_key(key)
        if index:
            return self._client.hset(index, key, value)
        return super().set(key, value, *args, **kwargs)

    def get(self, key: str, *args, **kwargs):
        index, key = self._get_index_and_key(key)
        if index:
            return self._client.hget(index, key)
        return super().get(key)

    def delete(self, key):
        index, key = self._get_index_and_key(key)
        if index:
            return self._client.hdel(index, key)
        return super().delete(key)

    def delete_match(self, pattern: str):
        index, key = self._get_index_and_key(pattern)
        if index:
            return self._client.unlink(index)
        return super().delete_match(pattern)
