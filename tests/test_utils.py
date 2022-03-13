from cashews.utils import Bitarray, get_hashes


def test_bitarray_get_size_1():
    b = Bitarray("100010", 2)
    assert b.get(0) == 0
    assert b.get(1) == 1
    assert b.get(2) == 0
    assert b.get(3) == 0
    assert b.get(4) == 0
    assert b.get(5) == 1
    assert b.get(6) == 0
    assert b.get(7) == 0


def test_bitarray_get_size_2():
    b = Bitarray("1000010", 2)
    assert b.get(0, 2) == 2
    assert b.get(1, 2) == 0
    assert b.get(2, 2) == 0
    assert b.get(3, 2) == 1
    assert b.get(4, 2) == 0


def test_bitarray_get_size_3():
    b = Bitarray("100010", 2)
    assert b.get(0, 3) == 2
    assert b.get(1, 3) == 4


def test_bitarray_incr_size_1():
    b = Bitarray("100010", 2)
    assert str(b.copy().incr(0)) == "100011"
    assert str(b.copy().incr(0, by=-1)) == "100010"
    assert str(b.copy().incr(1)) == "100010"
    assert str(b.copy().incr(1, by=-1)) == "100000"
    assert str(b.copy().incr(2)) == "100110"


def test_bitarray_incr_size_2():
    b = Bitarray("100010", 2)
    assert str(b.copy().incr(0, size=2)) == "100011"
    assert str(b.copy().incr(0, size=2, by=2)) == "100011"
    assert str(b.copy().incr(0, size=2, by=3)) == "100011"

    assert str(b.copy().incr(1, size=2)) == "100110"
    assert str(b.copy().incr(1, size=2, by=3)) == "101110"
    assert str(b.copy().incr(1, size=2, by=10)) == "101110"
    assert str(b.copy().incr(1, size=2, by=-1)) == "100010"


def test_bitarray_incr_size_3():
    b = Bitarray("100010", 2)
    assert str(b.copy().incr(0, size=3)) == "100011"
    assert str(b.copy().incr(0, size=3, by=2)) == "100100"
    assert str(b.copy().incr(0, size=3, by=3)) == "100101"
    assert str(b.copy().incr(0, size=3, by=6)) == "100111"


def test_bitarray_incr_over():
    b = Bitarray("1", 2)
    assert str(b.incr(2)) == "101"


def test_get_hashes_params():
    assert len(get_hashes("test", 3, 3)) == 3
    assert len(get_hashes("test", 1, 100)) == 1
    assert len(get_hashes("test", 77, 100)) == 77
    assert len(get_hashes("long string hash", 77, 100)) == 77
    assert get_hashes("test", 1, 100).pop() < 100


def test_get_hashes():
    assert get_hashes("test", 10, 1000) == get_hashes("test", 10, 1000)
    assert get_hashes("test", 2, 2) == get_hashes("test", 2, 2)

    assert get_hashes("test", 2, 5) != get_hashes("test", 2, 2)
    assert get_hashes("test", 3, 100) != get_hashes("tset", 3, 100)
    assert get_hashes("test", 3, 100) != get_hashes("t", 3, 100)
    assert get_hashes("test", 3, 100) != get_hashes("tes", 3, 100)
    assert get_hashes("test", 3, 100) != get_hashes("test2", 3, 100)
    assert not get_hashes("test", 5, 100).intersection(get_hashes("a", 5, 100))
    assert not get_hashes("test", 5, 100).intersection(get_hashes("test2", 5, 100))

    assert get_hashes("test", 3, 3) == get_hashes("a", 3, 3)  # it is ok to have collisions in this case
    assert len(get_hashes("test", 20, 100)) == len(set(get_hashes("test", 20, 100)))
