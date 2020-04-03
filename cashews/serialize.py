import hashlib
import hmac
import pickle


class UnSecureDataError(Exception):
    pass


class PickleSerializerMixin:
    def __init__(self, *args, hash_key=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._hash_key = hash_key

    async def get(self, key):
        return self._process(await super().get(key), key)

    def _process(self, value, key):
        if not value:
            return value
        try:
            sign, value = value.split(b"_", 1)
        except ValueError:
            raise UnSecureDataError()
        expected_sign = self.get_sign(key, value)
        if expected_sign != sign:
            raise UnSecureDataError()
        try:
            value = pickle.loads(value, fix_imports=False, encoding="bytes")
            repr(value)
            return value
        except (pickle.PickleError, AttributeError):
            return None

    async def get_many(self, *keys):
        return [self._process(value, key) for key, value in zip(keys, await super().get_many(*keys))]

    def get_sign(self, key: str, value: bytes) -> bytes:
        if self._hash_key is None:
            return b""
        value = key.encode() + value
        return hmac.new(self._hash_key, value, hashlib.sha1).hexdigest().encode()

    async def set(self, key: str, value, *args, **kwargs):
        value = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL, fix_imports=False)
        sign = self.get_sign(key, value)
        return await super().set(key, sign + b"_" + value, *args, **kwargs)
