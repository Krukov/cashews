from bitarray import bitarray, util


class Bitarray:
    __slots__ = ("_value",)

    def __init__(self, value, base=10):
        i = int(value, base=base)
        self._value = bitarray(f"{i:b}")

    def get(self, index: int, size: int = 1) -> int:
        start = max(len(self._value) - (index + 1) * size, 0)
        finish = max(len(self._value) - index * size, 0)
        bits = self._value[start:finish]
        if len(bits) == 0:
            return 0
        return util.ba2int(bits)

    def set(self, index: int, value: int, size: int = 1):
        for i in range(0, size):
            index_to_set = index * size + i
            if len(self._value) - 1 < index_to_set:
                self._value = bitarray((index_to_set - len(self._value) + 1) * "0" + self._value.to01())
            self._value[len(self._value) - index_to_set - 1] = (value >> i) & 1
        return

    def _set_bit_1(self, index):
        self._value[len(self._value) - index] = 1

    def _set_bit_0(self, index):
        self._value[len(self._value) - index] = 0

    def incr(self, index: int, size: int = 1, by: int = 1):
        if by > 0:
            by = min(by, 2**size - 1)
        else:
            by = max(by, -(2**size) - 1)
        value = self.get(index, size)
        value += by
        value = min(max(0, value), 2**size - 1)
        self.set(index, value, size)
        return self

    def copy(self):
        return Bitarray(self._value.to01(), base=2)

    def to_int(self):
        return util.ba2int(self._value, False)

    def __str__(self):
        return self._value.to01()
