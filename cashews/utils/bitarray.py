class Bitarray:
    __slots__ = ("_value", )

    def __init__(self, value, base=10):
        self._value = int(value, base)

    def get(self, index: int, size: int = 1) -> int:
        str_all = f"{self._value:b}"
        if (index + 1) * size > len(str_all):
            return 0
        end = len(str_all)
        str_int = str_all[end - (index + 1) * size : end - index * size]
        return int(str_int, 2)

    def incr(self, index: int, size: int = 1, by: int = 1):
        str_all = f"{self._value:b}"
        while (index + 1) * size > len(str_all):
            str_all = f"{0:0{size}b}" + str_all
        end = len(str_all)
        str_int = str_all[end - (index + 1) * size : end - index * size]
        max_value = 2 ** size - 1
        value = max(min(int(str_int, 2) + by, max_value), 0)
        str_int = str_all[: end - (index + 1) * size] + f"{value:0{size}b}" + str_all[end - index * size :]
        self._value = int(str_int, 2)
        return self

    def copy(self):
        return Bitarray(str(self), 2)

    def __str__(self):
        return f"{self._value:b}"
