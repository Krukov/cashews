from __future__ import annotations

import zlib
from collections.abc import MutableSequence

algorithms: MutableSequence = [
    zlib.crc32,
]

try:
    import xxhash
except ImportError:
    pass
else:
    algorithms.extend(
        [
            xxhash.xxh3_64_intdigest,
            xxhash.xxh64_intdigest,
            xxhash.xxh3_128_intdigest,
            xxhash.xxh32_intdigest,
        ]
    )


def get_indexes(key: str, number_of_buckets: int, max_index: int) -> set[int]:
    """
    return array with bit indexes for given value (key) [23, 45, 15]
    """

    assert max_index >= number_of_buckets

    indexes = set()
    for i in range(number_of_buckets):
        ii = i % len(algorithms)

        value = algorithms[ii](f"{key}_{i}".encode()) % max_index
        while value in indexes:
            i += 1
            value = algorithms[ii](f"{key}_{i}".encode()) % max_index
        indexes.add(value)
    return indexes
