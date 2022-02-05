import hashlib


def get_hashes(key: str, k: int, max_i: int):
    """
    return array with bit indexes for given value (key) [23, 45, 15]
    """
    assert max_i >= k
    str_hash = str(_get_string_int_hash(key))
    indexes = set()
    for _hash in _split_string_for_chunks(str_hash, k):
        value = int(_hash) % max_i
        while value in indexes:
            value += 1
        indexes.add(value)
    return indexes


def _get_string_int_hash(key):
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest(), 16)


def _split_string_for_chunks(value: str, chunks: int):
    chunk_size = len(value) // chunks
    return [value[i : i + chunk_size] for i in range(0, chunk_size * chunks, chunk_size)]

