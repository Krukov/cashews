import hashlib
import hmac
import pickle


class UnSecureDataError(Exception):
    pass


class none:
    pass


class PickleSerializerMixin:
    _digestmods = {
        b"sha1": hashlib.sha1,
        b"md5": hashlib.md5,
        b"sha256": hashlib.sha256,
    }

    def __init__(self, *args, hash_key=None, digestmod=b"md5", **kwargs):
        super().__init__(*args, **kwargs)
        if isinstance(hash_key, str):
            hash_key = hash_key.encode()

        self._hash_key = hash_key
        if isinstance(digestmod, str):
            digestmod = digestmod.encode()
        self._digestmod = digestmod

    async def get(self, key, default=None):
        try:
            return self._process_value(await super().get(key), key, default=default)
        except UnSecureDataError:
            await super().delete(key)
            raise
        except (pickle.PickleError, AttributeError):
            await super().delete(key)
            return default

    def _process_value(self, value: bytes, key, default=None):
        if value is None:
            return default
        if isinstance(value, int) or value.isdigit():
            return int(value)
        try:
            sign, value = value.split(b"_", 1)
        except ValueError:
            raise UnSecureDataError()
        sign, digestmod = self._get_digestmod(sign)
        expected_sign = self.get_sign(key, value, digestmod)
        if expected_sign != sign:
            raise UnSecureDataError()
        value = pickle.loads(value, fix_imports=False, encoding="bytes")
        repr(value)
        if value is none:
            return None
        return value

    async def get_many(self, *keys):
        values = []
        for key, value in zip(keys, await super().get_many(*keys) or [None] * len(keys)):
            try:
                values.append(self._process_value(value, key))
            except UnSecureDataError:
                await super().delete(key)
                raise
            except (pickle.PickleError, AttributeError):
                await super().delete(key)
                values.append(None)
        return tuple(values)

    def get_sign(self, key: str, value: bytes, digestmod: bytes) -> bytes:
        if self._hash_key is None:
            return b""
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
        if value is None:
            value = none
        if isinstance(value, int) and not isinstance(value, bool):
            return await super().set(key, value, *args, **kwargs)
        value = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL, fix_imports=False)
        sign = self.get_sign(key, value, self._digestmod)
        return await super().set(key, self._digestmod + b":" + sign + b"_" + value, *args, **kwargs)

    def set_row(self, *args, **kwargs):
        return super().set(*args, **kwargs)

    def get_row(self, *args, **kwargs):
        return super().get(*args, **kwargs)
