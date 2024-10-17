from __future__ import annotations

import hashlib
import hmac
from typing import TYPE_CHECKING, Any, Mapping

from .exceptions import SignIsMissingError, UnSecureDataError
from .picklers import DEFAULT_PICKLE, NULL_PICKLE, Pickler, get_pickler

if TYPE_CHECKING:  # pragma: no cover
    from ._typing import ICustomDecoder, ICustomEncoder, Key, Value
    from .backends.interface import Backend


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
        secret: str | bytes | None = None,
        digestmod: str | bytes = b"md5",
        check_repr: bool = True,
        pickle_type: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self._serializer = Serializer(check_repr=check_repr)
        if secret:
            self._serializer.set_signer(HashSigner(secret, digestmod))

        self._serializer.set_pickler(self._get_pickler(pickle_type, bool(secret)))

    @classmethod
    def _get_pickler(cls, pickle_type: str | None, hash_key: bool) -> Pickler:
        pickle_type = pickle_type or cls.pickle_type
        if pickle_type is NULL_PICKLE and hash_key:
            pickle_type = DEFAULT_PICKLE
        return get_pickler(pickle_type)

    async def get(self, key: Key, default: Value | None = None):
        raw_value = await super().get(key, default=default)  # type: ignore[misc]
        return await self._serializer.decode(self, key, raw_value, default=default)  # type: ignore[arg-type]

    async def get_many(self, *keys: Key, default: Value | None = None) -> Value:
        encoded_values = await super().get_many(*keys, default=default)  # type: ignore[misc]
        values = []
        for key, value in zip(keys, encoded_values):
            deserialized_value = await self._serializer.decode(
                backend=self,  # type: ignore[arg-type]
                key=key,
                value=value,
                default=default,
            )
            values.append(deserialized_value)
        return tuple(values)

    async def set(
        self,
        key: Key,
        value: Value,
        expire: float | None = None,
        exist: bool | None = None,
    ):
        value = await self._serializer.encode(self, key, value, expire=expire)  # type: ignore[arg-type]
        return await super().set(key, value, expire=expire, exist=exist)  # type: ignore[misc]

    async def set_many(self, pairs: Mapping[Key, Value], expire: float | None = None):
        transformed_pairs = {}
        for key, value in pairs.items():
            transformed_pairs[key] = await self._serializer.encode(self, key, value, expire)  # type: ignore[arg-type]
        return await super().set_many(transformed_pairs, expire=expire)  # type: ignore[misc]

    def set_raw(self, *args: Any, **kwargs: Any):
        return super().set(*args, **kwargs)  # type: ignore

    def get_raw(self, *args: Any, **kwargs: Any):
        return super().get(*args, **kwargs)  # type: ignore


def _to_bytes(value: str | bytes) -> bytes:
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

    def __init__(self, secret: str | bytes, digestmod: str | bytes = b"md5"):
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

    def _get_sign_and_digestmod(self, sign: bytes) -> tuple[bytes, bytes]:
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
    _type_mapping: dict[bytes, tuple[ICustomEncoder, ICustomDecoder]] = {}

    def __init__(self, check_repr=False):
        self._check_repr = check_repr
        self._pickler = get_pickler(NULL_PICKLE)
        self._signer = NullSigner()

    def set_signer(self, signer):
        self._signer = signer

    def set_pickler(self, pickler):
        self._pickler = pickler

    @classmethod
    def register_type(cls, klass: type, encoder, decoder):
        cls._type_mapping[bytes(klass.__name__, "utf8")] = (encoder, decoder)

    async def encode(self, backend: Backend, key: Key, value: Value, expire: float | None) -> bytes:  # on SET
        if isinstance(value, int) and not isinstance(value, bool):
            return value  # type: ignore[return-value]
        _value = await self._custom_encode(backend, key, value, expire)
        if _value is not None:
            return self._signer.sign(key, _value)
        return self._signer.sign(key, self._pickler.dumps(value))

    async def _custom_encode(self, backend, key: Key, value: Value, expire: float | None) -> bytes | None:
        value_type = bytes(type(value).__name__, "utf8")
        if value_type not in self._type_mapping:
            return None
        encoder, _ = self._type_mapping[value_type]
        encoded_value = await encoder(value, backend, key, expire)
        return value_type + b":" + encoded_value

    async def decode(self, backend: Backend, key: Key, value: bytes, default: Value) -> Value:  # on GET
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

    async def _custom_decode(self, backend: Backend, key: Key, value: bytes, default: Value) -> Value:
        try:
            value_type, value = value.split(b":", 1)
        except ValueError:
            return default
        if value_type not in self._type_mapping:
            return default
        _, decoder = self._type_mapping[value_type]
        try:
            return await decoder(value, backend, key)
        except DecodeError:
            return default


class DecodeError(Exception):
    pass


register_type = Serializer.register_type


async def bytes_encoder(value: bytes, *args, **kwargs):
    return value


async def bytes_decoder(value: bytes, *args, **kwargs):
    return value


register_type(bytes, bytes_encoder, bytes_decoder)
