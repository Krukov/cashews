import hashlib
import hmac
import pickle
from contextlib import suppress
from typing import Any, Optional, Tuple, Union

BLANK_DIGEST = b""


class UnSecureDataError(Exception):
    pass


class SignIsMissingError(Exception):
    ...


class PickleSerializerMixin:
    _digestmods = {
        b"sha1": hashlib.sha1,
        b"md5": hashlib.md5,
        b"sha256": hashlib.sha256,
    }

    def __init__(
        self,
        *args,
        hash_key: Union[str, bytes, None] = None,
        digestmod: Union[str, bytes, None] = b"md5",
        check_repr: bool = True,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if hash_key is None:
            digestmod = BLANK_DIGEST
            self._digestmods = self._digestmods.copy()
            self._digestmods[BLANK_DIGEST] = lambda value: BLANK_DIGEST
        self._hash_key = _to_bytes(hash_key)
        self._digestmod = _to_bytes(digestmod)
        self._check_repr = check_repr

    async def get(self, key: str, default: Any = None):
        return await self._get_value(await super().get(key), key, default=default)

    async def _get_value(self, value, key, default=None) -> Any:
        try:
            return self._process_value(value, key, default=default)
        except (pickle.PickleError, AttributeError):
            return default

    def _process_value(self, value: Union[None, int, bytes], key: str, default=None) -> Any:
        if value is None:
            return default
        if isinstance(value, int):
            return value
        if value.isdigit():
            return int(value)
        try:
            value = self._split_value_from_signature(value, key)
        except SignIsMissingError:
            return value
        value = pickle.loads(value, fix_imports=False, encoding="bytes")
        if self._check_repr:
            repr(value)
        return value

    def _split_value_from_signature(self, value: bytes, key: str) -> bytes:
        if self._hash_key:
            try:
                sign, value = value.split(b"_", 1)
            except ValueError:
                raise SignIsMissingError(f"key: {key}")
            sign, digestmod = self._get_digestmod(sign)
            expected_sign = self._get_sign(key, value, digestmod)
            if expected_sign != sign:
                raise UnSecureDataError(f"{expected_sign!r} != {sign!r}")
            return value
        else:
            # Backward compatibility.
            DeprecationWarning(
                "If a sign is not used to secure your data, then a value will be pickled and saved without an empty sign prepended."
                "Values saved via 4.x package version without using a sign will not be compatible after the 5.x release."
            )
            with suppress(ValueError):
                sign, value = value.split(b"_", 1)
                if sign:
                    value = sign + b"_" + value
            return value

    def _get_digestmod(self, sign: bytes) -> Tuple[bytes, bytes]:
        digestmod = self._digestmod
        if b":" in sign:
            digestmod, sign = sign.split(b":")
        if digestmod not in self._digestmods:
            raise UnSecureDataError()
        return sign, digestmod

    async def get_many(self, *keys: str, default: Optional[Any] = None) -> Any:
        values = []
        for key, value in zip(keys, await super().get_many(*keys, default=default) or [None] * len(keys)):
            values.append(await self._get_value(value, key))
        return tuple(values)

    async def set(self, key: str, value: Any, *args, **kwargs):
        if isinstance(value, int) and not isinstance(value, bool):
            return await super().set(key, value, *args, **kwargs)
        value = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL, fix_imports=False)
        return await super().set(key, self._prepend_sign_to_value(key, value), *args, **kwargs)

    def _prepend_sign_to_value(self, key: str, value: bytes) -> bytes:
        sign = self._get_sign(key, value, self._digestmod)
        if not sign:
            return value
        return self._digestmod + b":" + sign + b"_" + value

    def _get_sign(self, key: str, value: bytes, digestmod: bytes) -> bytes:
        if digestmod == BLANK_DIGEST:
            return BLANK_DIGEST
        value = key.encode() + value
        return hmac.new(self._hash_key, value, self._digestmods[digestmod]).hexdigest().encode()

    def set_raw(self, *args, **kwargs):
        return super().set(*args, **kwargs)

    def get_raw(self, *args, **kwargs):
        return super().get(*args, **kwargs)


def _to_bytes(value: Union[str, bytes, None]) -> Optional[bytes]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.encode()
    return value
