from __future__ import annotations

import gzip
import zlib
from enum import Enum

from .exceptions import DecompressionError, UnsupportedCompressorError


class CompressType(Enum):
    NULL = "null"
    GZIP = "gzip"
    ZLIB = "zlib"


class Compressor:
    @staticmethod
    def compress(value: bytes) -> bytes:
        return value

    @staticmethod
    def decompress(value: bytes) -> bytes:
        return value


class GzipCompressor(Compressor):
    @staticmethod
    def compress(value: bytes) -> bytes:
        return gzip.compress(value, compresslevel=9)

    @staticmethod
    def decompress(value: bytes) -> bytes:
        try:
            return gzip.decompress(value)
        except gzip.BadGzipFile as exc:
            raise DecompressionError from exc


class ZlibCompressor(Compressor):
    @staticmethod
    def compress(value: bytes) -> bytes:
        return zlib.compress(value)

    @staticmethod
    def decompress(value: bytes) -> bytes:
        try:
            return zlib.decompress(value)
        except zlib.error as exc:
            raise DecompressionError from exc


_compressors = {
    CompressType.NULL: Compressor,
    CompressType.GZIP: GzipCompressor,
    CompressType.ZLIB: ZlibCompressor,
}


def get_compressor(compress_type: CompressType | None) -> type[Compressor]:
    if compress_type is None:
        return Compressor
    if compress_type not in _compressors:
        raise UnsupportedCompressorError
    return _compressors[compress_type]
