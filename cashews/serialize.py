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
        value = await super().get(key)
        if not value:
            return value
        try:
            sign, value = value.split(b"_", 1)
        except ValueError:
            raise UnSecureDataError()
        expected_sign = self.get_sign(value)
        if expected_sign != sign:
            raise UnSecureDataError()
        try:
            return pickle.loads(value, fix_imports=False, encoding="bytes")
        except pickle.PickleError:
            return None

    def get_sign(self, value: bytes):
        return hmac.new(self._hash_key, value, hashlib.md5).hexdigest().encode()

    async def set(self, key: str, value, *args, **kwargs):
        value = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL, fix_imports=False)
        sign = self.get_sign(value)
        return await super().set(key, sign + b"_" + value, *args, **kwargs)
