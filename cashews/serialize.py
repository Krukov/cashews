import hashlib
import hmac
from typing import Any, Mapping, Optional, Tuple, Union

from .exceptions import SignIsMissingError, UnSecureDataError
from .picklers import DEFAULT_PICKLE, get_pickler


def get_signer(digestmod):
    def sign(key: bytes, value: bytes):
        return hmac.new(key, value, digestmod).hexdigest().encode()

    return sign


def simple_sign(key: bytes, value: bytes):
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

    async def get(self, key: str, default: Any = None):
        return await self._serialize_value(await super().get(key), key, default=default)

    async def _serialize_value(self, value: Union[None, int, bytes], key: str, default=None) -> Any:
        if value is None:
            return default
        if isinstance(value, int):
            return value
        if value.isdigit():
            return int(value)
        try:
            return self._process_value(value, key, default=default)
        except (self._pickler.PickleError, AttributeError):
            return default

    def _process_value(self, value: bytes, key: str, default=None) -> Any:
        if not self._hash_key:
            try:
                return self._process_only_value(value)
            except self._pickler.PickleError:
                pass
        try:
            value = self._get_value_without_signature(value, key)
        except SignIsMissingError:
            return default
        return self._process_only_value(value)

    def _process_only_value(self, value: bytes) -> Any:
        value = self._pickler.loads(value)
        if self._check_repr:
            repr(value)
        return value

    def _get_value_without_signature(self, value: bytes, key: str) -> bytes:
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

    async def get_many(self, *keys: str, default: Optional[Any] = None) -> Any:
        values = []
        for key, value in zip(keys, await super().get_many(*keys, default=default) or [None] * len(keys)):
            values.append(await self._serialize_value(value, key))
        return tuple(values)

    async def set(self, key: str, value: Any, *args: Any, **kwargs: Any):
        if isinstance(value, int) and not isinstance(value, bool):
            return await super().set(key, value, *args, **kwargs)
        value = self._pickler.dumps(value)
        return await super().set(key, self._prepend_sign_to_value(key, value), *args, **kwargs)

    async def set_many(self, pairs: Mapping[str, Any], expire: Optional[float] = None):
        transformed_pairs = {}
        for key, value in pairs.items():
            if isinstance(value, int) and not isinstance(value, bool):
                transformed_pairs[key] = value
                continue
            value = self._pickler.dumps(value)
            transformed_pairs[key] = self._prepend_sign_to_value(key, value)
        return await super().set_many(transformed_pairs, expire=expire)

    def _prepend_sign_to_value(self, key: str, value: bytes) -> bytes:
        sign = self._gen_sign(key, value, self._digestmod)
        if not sign:
            return value
        return self._digestmod + b":" + sign + b"_" + value

    def _gen_sign(self, key: str, value: bytes, digestmod: bytes) -> bytes:
        if self._hash_key is None:
            return b""
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
