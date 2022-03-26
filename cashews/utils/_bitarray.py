class Bitarray:
    __slots__ = ("_value",)

    def __init__(self, value, base=10):
        self._value = int(value, base=base)

    def get(self, index: int, size: int = 1) -> int:
        res = 0
        bit_index = 0
        for i in range(index * size, (index + 1) * size):
            res |= ((self._value >> i) & 1) << bit_index
            bit_index += 1
        return res

    def set(self, index: int, value: int, size: int = 1):
        for i in range(0, size):
            if (value >> i) & 1:
                self._set_bit_1(index * size + i)
            else:
                self._set_bit_0(index * size + i)
        return

    def _set_bit_1(self, index):
        self._value |= 1 << index

    def _set_bit_0(self, index):
        self._value &= ~(1 << index)

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
        return Bitarray(str(self), 2)

    def to_int(self):
        return self._value

    def __str__(self):
        return f"{self._value:b}"
