import hashlib
import hmac
from typing import Any, Mapping, Optional, Tuple, Type, Union

from ._typing import Key, Value
from .exceptions import SignIsMissingError, UnSecureDataError
from .picklers import DEFAULT_PICKLE, get_pickler

_default = object()


def get_signer(digestmod):
    def sign(key: bytes, value: bytes) -> bytes:
        return hmac.new(key, value, digestmod).hexdigest().encode()

    return sign


def simple_sign(key: bytes, value: bytes) -> bytes:
    s = sum(key) + sum(value)
    return f"{s:x}".encode()


class PickleSerializerMixin:
    _digestmods = {
        b"sha1": get_signer(hashlib.sha1),
        b"md5": get_signer(hashlib.md5),
        b"sha256": get_signer(hashlib.sha256),
        b"sum": simple_sign,
    }

    def __init__(
        self,
        *args,
        hash_key: Union[str, bytes, None] = None,
        digestmod: Union[str, bytes, None] = b"md5",
        check_repr: bool = True,
        pickle_type: str = DEFAULT_PICKLE,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self._hash_key = _to_bytes(hash_key)
        self._digestmod = _to_bytes(digestmod)
        self._check_repr = check_repr
        self._pickler = get_pickler(pickle_type)

    async def get(self, key: Key, default: Optional[Value] = None):
        raw_value = await super().get(key)
        return await self._deserialize_value(raw_value, key, default=default)

    def _deserialize_value(self, value: Union[None, int, bytes], key: Key, default=None) -> Value:
        if value is None or value is default:
            return default
        if isinstance(value, int):
            return value
        if value.isdigit():
            return int(value)
        try:
            return await self._process_value(value, key, default=default)
        except (self._pickler.PickleError, AttributeError):
            return default

    async def _process_value(self, value: bytes, key: Key, default=None) -> Value:
        if not self._hash_key:
            try:
                return self._process_only_value(value)
            except self._pickler.PickleError:
                value = await custom_serializer.decode(value, self, key, _default)
                if value is not _default:
                    return value
        try:
            value = self._get_value_without_signature(value, key)
        except SignIsMissingError:
            return default
        return self._process_only_value(value)

    def _process_only_value(self, value: bytes) -> Value:
        value = self._pickler.loads(value)
        if self._check_repr:
            repr(value)
        return value

    def _get_value_without_signature(self, value: bytes, key: Key) -> bytes:
        try:
            sign, value = value.split(b"_", 1)
        except ValueError as exc:
            raise SignIsMissingError(f"key: {key}") from exc
        if not self._hash_key:
            return value
        sign, digestmod = self._get_sign_and_digestmod(sign)
        expected_sign = self._gen_sign(key, value, digestmod)
        if expected_sign != sign:
            raise UnSecureDataError(f"{expected_sign!r} != {sign!r}")
        return value

    def _get_sign_and_digestmod(self, sign: bytes) -> Tuple[bytes, bytes]:
        digestmod = self._digestmod
        if b":" in sign:
            digestmod, sign = sign.split(b":")
        if digestmod not in self._digestmods:
            raise UnSecureDataError()
        return sign, digestmod

    async def get_many(self, *keys: Key, default: Optional[Value] = None) -> Value:
        encoded_values = await super().get_many(*keys, default=default)
        values = []
        for key, value in zip(keys, encoded_values):
            deserialized_value = self._deserialize_value(value, key, default=default)
            values.append(deserialized_value)
        return tuple(values)

    async def set(
        self,
        key: Key,
        value: Value,
        expire: Optional[float] = None,
        exist: Optional[bool] = None,
    ):
        value = await self._serialize_value(value, key, expire=expire)
        return await super().set(key, value, expire=expire, exist=exist)

    async def set_many(self, pairs: Mapping[Key, Value], expire: Optional[float] = None):
        transformed_pairs = {}
        for key, value in pairs.items():
            transformed_pairs[key] = await self._serialize_value(value, key, expire)
        return await super().set_many(transformed_pairs, expire=expire)

    async def _serialize_value(self, value: Any, key: str, expire) -> bytes:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        try:
            value = self._pickler.dumps(value)
        except self._pickler.UnpicklingError:
            value = await custom_serializer.encode(value, self, key, expire)
            if value is None:
                raise
            return value
        return self._prepend_sign_to_value(key, value)

    def _prepend_sign_to_value(self, key: Key, value: Value) -> bytes:
        if self._hash_key is None:
            return value
        sign = self._gen_sign(key, value, self._digestmod)
        return self._digestmod + b":" + sign + b"_" + value

    def _gen_sign(self, key: Key, value: bytes, digestmod: bytes) -> bytes:
        value = key.encode() + value
        return self._digestmods[digestmod](self._hash_key, value)

    def set_raw(self, *args: Any, **kwargs: Any):
        return super().set(*args, **kwargs)

    def get_raw(self, *args: Any, **kwargs: Any):
        return super().get(*args, **kwargs)


def _to_bytes(value: Union[str, bytes, None]) -> Optional[bytes]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.encode()
    return value


class CustomSerializer:
    def __init__(self):
        self._type_mapping = {}

    def register_type(self, klass: Type, encoder, decoder):
        self._type_mapping[bytes(klass.__name__, "utf8")] = (encoder, decoder)

    async def encode(self, value: Any, backend, key, ttl: int) -> Optional[bytes]:
        value_type = bytes(type(value).__name__, "utf8")
        if value_type not in self._type_mapping:
            return
        encoder, _ = self._type_mapping[value_type]
        encoded_value = await encoder(value, backend, key, ttl)
        return value_type + b":" + encoded_value

    async def decode(self, value: bytes, backend, key: str, default) -> Any:
        try:
            value_type, value = value.split(b":", 1)
        except ValueError:
            return default
        if value_type not in self._type_mapping:
            return default
        _, decoder = self._type_mapping[value_type]
        decode_value = await decoder(value, backend, key)
        return decode_value


custom_serializer = CustomSerializer()
