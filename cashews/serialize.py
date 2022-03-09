import hashlib
import hmac
import pickle
from typing import Union

BLANK_DIGEST = b""


class UnSecureDataError(Exception):
    pass


class PickleSerializerMixin:
    _digestmods = {
        b"sha1": hashlib.sha1,
        b"md5": hashlib.md5,
        b"sha256": hashlib.sha256,
    }

    def __init__(self, *args, hash_key=None, digestmod=b"md5", check_repr=True, **kwargs):
        super().__init__(*args, **kwargs)
        if hash_key is None:
            digestmod = BLANK_DIGEST
            self._digestmods = self._digestmods.copy()
            self._digestmods[BLANK_DIGEST] = lambda value: BLANK_DIGEST
        self._hash_key = _to_bytes(hash_key)
        self._digestmod = _to_bytes(digestmod)
        self._check_repr = check_repr

    async def get(self, key: str, default=None):
        return await self._get_value(await super().get(key), key, default=default)

    async def _get_value(self, value, key, default=None):
        try:
            return self._process_value(value, key, default=default)
        except UnSecureDataError:
            await super().delete(key)
            raise
        except (pickle.PickleError, AttributeError):
            await super().delete(key)
            return default

    def _process_value(self, value: Union[bytes, None, int, str], key, default=None):
        if value is None:
            return default
        if isinstance(value, int):
            return value
        if value.isdigit():
            return int(value)
        try:
            sign, value = value.split(b"_", 1)
        except ValueError:
            return value
        sign, digestmod = self._get_digestmod(sign)
        expected_sign = self.get_sign(key, value, digestmod)
        if expected_sign != sign:
            raise UnSecureDataError()
        value = pickle.loads(value, fix_imports=False, encoding="bytes")
        if self._check_repr:
            repr(value)
        return value

    async def get_many(self, *keys):
        values = []
        for key, value in zip(keys, await super().get_many(*keys) or [None] * len(keys)):
            values.append(await self._get_value(value, key))

        return tuple(values)

    def get_sign(self, key: str, value: bytes, digestmod: bytes) -> bytes:
        if digestmod == BLANK_DIGEST:
            return BLANK_DIGEST
        value = key.encode() + value
        return hmac.new(self._hash_key, value, self._digestmods[digestmod]).hexdigest().encode()

    def _get_digestmod(self, sign: bytes):
        digestmod = self._digestmod
        if b":" in sign:
            digestmod, sign = sign.split(b":")
        if digestmod not in self._digestmods:
            raise UnSecureDataError()
        return sign, digestmod

    async def set(self, key: str, value, *args, **kwargs):
        if isinstance(value, int) and not isinstance(value, bool):
            return await super().set(key, value, *args, **kwargs)
        value = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL, fix_imports=False)
        sign = self.get_sign(key, value, self._digestmod)
        return await super().set(key, self._digestmod + b":" + sign + b"_" + value, *args, **kwargs)

    def set_raw(self, *args, **kwargs):
        return super().set(*args, **kwargs)

    def get_raw(self, *args, **kwargs):
        return super().get(*args, **kwargs)


def _to_bytes(value) -> bytes:
    if isinstance(value, str):
        value = value.encode()
    return value
