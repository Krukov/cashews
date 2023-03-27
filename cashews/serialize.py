import hashlib
import hmac
import warnings
from typing import TYPE_CHECKING, Any, Mapping, Optional, Tuple, Type, Union

from ._typing import Key, Value
from .exceptions import SignIsMissingError, UnSecureDataError
from .picklers import DEFAULT_PICKLE, NULL_PICKLE, Pickler, get_pickler

if TYPE_CHECKING:
    from .backends.interface import Backend

_empty = object()


def _seal(digestmod):
    def sign(key: bytes, value: bytes) -> bytes:
        return hmac.new(key, value, digestmod).hexdigest().encode()

    return sign


def simple_sign(key: bytes, value: bytes) -> bytes:
    s = sum(key) + sum(value)
    return f"{s:x}".encode()


class SerializerMixin:
    pickle_type = NULL_PICKLE

    def __init__(
        self,
        *args,
        hash_key: Union[str, bytes, None] = _empty,
        secret: Union[str, bytes, None] = None,
        digestmod: Union[str, bytes, None] = b"md5",
        check_repr: bool = True,
        pickle_type: Optional[str] = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        if hash_key is not _empty:
            warnings.warn(
                "`hash_key` property was renamed to `secret` and will be removed in next release",
                DeprecationWarning,
            )
            secret = hash_key

        self._serializer = Serializer(check_repr=check_repr)
        if secret:
            self._serializer.set_signer(HashSigner(secret, digestmod))

        self._serializer.set_pickler(self._get_pickler(pickle_type, secret))

    @classmethod
    def _get_pickler(cls, pickle_type: Optional[str], hash_key: bool) -> Pickler:
        pickle_type = pickle_type or cls.pickle_type
        if pickle_type is NULL_PICKLE and hash_key:
            pickle_type = DEFAULT_PICKLE
        return get_pickler(pickle_type)

    async def get(self, key: Key, default: Optional[Value] = None):
        raw_value = await super().get(key, default=default)
        return await self._serializer.decode(self, key, raw_value, default=default)

    async def get_many(self, *keys: Key, default: Optional[Value] = None) -> Value:
        encoded_values = await super().get_many(*keys, default=default)
        values = []
        for key, value in zip(keys, encoded_values):
            deserialized_value = await self._serializer.decode(self, key, value, default=default)
            values.append(deserialized_value)
        return tuple(values)

    async def set(
        self,
        key: Key,
        value: Value,
        expire: Optional[float] = None,
        exist: Optional[bool] = None,
    ):
        value = await self._serializer.encode(self, key, value, expire=expire)
        return await super().set(key, value, expire=expire, exist=exist)

    async def set_many(self, pairs: Mapping[Key, Value], expire: Optional[float] = None):
        transformed_pairs = {}
        for key, value in pairs.items():
            transformed_pairs[key] = await self._serializer.encode(self, key, value, expire)
        return await super().set_many(transformed_pairs, expire=expire)

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


class HashSigner:
    _digestmods = {
        b"sha1": _seal(hashlib.sha1),
        b"md5": _seal(hashlib.md5),
        b"sha256": _seal(hashlib.sha256),
        b"sum": simple_sign,
    }

    def __init__(self, secret: Union[str, bytes], digestmod: Union[str, bytes] = b"md5"):
        self._secret = _to_bytes(secret)
        self._digestmod = _to_bytes(digestmod)

    def sign(self, key: Key, value: bytes) -> bytes:
        sign = self._gen_sign(key, value, self._digestmod)
        return self._digestmod + b":" + sign + b"_" + value

    def check_sign(self, key: Key, value: bytes) -> bytes:
        try:
            sign, value = value.split(b"_", 1)
        except ValueError as exc:
            raise SignIsMissingError(f"key: {key}") from exc

        sign, digestmod = self._get_sign_and_digestmod(sign)
        expected_sign = self._gen_sign(key, value, digestmod)
        if expected_sign != sign:
            raise UnSecureDataError(f"{expected_sign!r} != {sign!r}")
        return value

    def _gen_sign(self, key: Key, value: bytes, digestmod: bytes) -> bytes:
        value = key.encode() + value
        return self._digestmods[digestmod](self._secret, value)

    def _get_sign_and_digestmod(self, sign: bytes) -> Tuple[bytes, bytes]:
        digestmod = self._digestmod
        if b":" in sign:
            digestmod, sign = sign.split(b":")
        if digestmod not in self._digestmods:
            raise UnSecureDataError()
        return sign, digestmod


class NullSigner:
    @staticmethod
    def sign(key: Key, value: bytes) -> bytes:
        return value

    @staticmethod
    def check_sign(key: Key, value: bytes) -> bytes:
        return value


class Serializer:
    _type_mapping = {}

    def __init__(self, check_repr=False):
        self._check_repr = check_repr
        self._pickler = get_pickler(NULL_PICKLE)
        self._signer = NullSigner()

    def set_signer(self, signer):
        self._signer = signer

    def set_pickler(self, pickler):
        self._pickler = pickler

    @classmethod
    def register_type(cls, klass: Type, encoder, decoder):
        cls._type_mapping[bytes(klass.__name__, "utf8")] = (encoder, decoder)

    async def encode(self, backend: "Backend", key: Key, value: Value, expire: int) -> bytes:  # on SET
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        _value = await self._custom_encode(backend, key, value, expire)
        if _value is not None:
            return self._signer.sign(key, _value)
        return self._signer.sign(key, self._pickler.dumps(value))

    async def _custom_encode(self, backend, key: Key, value: Value, expire: int) -> Optional[bytes]:
        value_type = bytes(type(value).__name__, "utf8")
        if value_type not in self._type_mapping:
            return
        encoder, _ = self._type_mapping[value_type]
        encoded_value = await encoder(value, backend, key, expire)
        return value_type + b":" + encoded_value

    async def decode(self, backend: "Backend", key: Key, value: bytes, default: Value) -> Value:  # on GET
        if value is default:
            return default
        if not isinstance(value, bytes):
            return value
        if value.isdigit():
            return int(value)
        try:
            value = self._signer.check_sign(key, value)
        except SignIsMissingError:
            return default

        try:
            value = self._decode(value)
        except self._pickler.UnpicklingError:
            pass
        except AttributeError:
            return default
        if isinstance(value, bytes):
            return await self._custom_decode(backend, key, value, default)
        return value

    def _decode(self, value: bytes) -> Value:
        value = self._pickler.loads(value)
        if self._check_repr:
            repr(value)
        return value

    async def _custom_decode(self, backend: "Backend", key: Key, value: bytes, default: Value) -> Value:
        try:
            value_type, value = value.split(b":", 1)
        except ValueError:
            return default
        if value_type not in self._type_mapping:
            return default
        _, decoder = self._type_mapping[value_type]
        decode_value = await decoder(value, backend, key)
        return decode_value


register_type = Serializer.register_type


async def bytes_encoder(value: bytes, *args, **kwargs):
    return value


async def bytes_decoder(value: bytes, *args, **kwargs):
    return value


register_type(bytes, bytes_encoder, bytes_decoder)
