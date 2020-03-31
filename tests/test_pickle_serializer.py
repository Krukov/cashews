import dataclasses
from collections import namedtuple
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from cashews.serialize import PickleSerializerMixin, UnSecureDataError

pytestmark = pytest.mark.asyncio


@dataclasses.dataclass()
class TestDC:
    test: str
    _: int


NT = namedtuple("NT", ("test", "name"), defaults=[False, "test"])


class DummyCache:
    def __init__(self):
        self.store = {}

    async def set(self, key: str, value, *args, **kwargs) -> None:
        self.store[key] = value

    async def get(self, key: str, *args, **kwargs):
        return self.store.get(key, None)

    async def get_many(self, *keys):
        return [self.store.get(key, None) for key in keys]


class Cache(PickleSerializerMixin, DummyCache):
    pass


@pytest.fixture(name="cache")
def _cache():
    cache = Cache()
    cache._hash_key = b"test"
    return cache


@pytest.mark.parametrize(
    "value",
    (
        "test",
        b"test",
        b"1_1",
        1,
        1.234,
        Decimal("1.001"),
        True,
        False,
        "",
        0,
        None,
        {"hi": True},
        TestDC(test="test", _=1),
    ),
)
async def test_serialize_simple_value(value, cache):
    await cache.set("key", value)
    assert await cache.get("key") == value
    assert await cache.get_many("key") == [
        value,
    ]


@pytest.mark.parametrize("value", (NT(name="test", test="lol"), NT()))
async def test_serialize_collections_value(value, cache):
    await cache.set("key", value)
    assert await cache.get("key") == value
    assert (await cache.get("key")).name == "test"


@pytest.mark.parametrize(
    "value", (datetime(year=2000, month=1, day=10, hour=10), timedelta(days=10), date(year=2020, month=12, day=31))
)
async def test_serialize_dates_value(value, cache):
    await cache.set("key", value)
    assert await cache.get("key") == value


@pytest.mark.parametrize(
    "value", (["test", "to"], (TestDC("hay", _=2),), {"test"}, [(1, 2), (3, 4)], {1, 2, 5}, [{"test": True}])
)
async def test_serialize_array_value(value, cache):
    await cache.set("key", value)
    assert list(await cache.get("key")) == list(value)
    assert type(await cache.get("key")) == type(value)


@pytest.mark.parametrize(
    "value", (["test", b"to"], (TestDC("hay", _=2), Decimal("10.1")), {"test", 1}, [], [{1: True}, Decimal("0.1")])
)
async def test_serialize_array_diff_value(value, cache):
    await cache.set("key", value)
    assert list(await cache.get("key")) == list(value)
    assert type(await cache.get("key")) == type(value)


async def test_unsecure_value(cache):
    cache.store["key"] = b"cos\nsystem\n(S'echo hello world'\ntR."
    with pytest.raises(UnSecureDataError):
        await cache.get("key")

    cache.store["key"] = b"_cos\nsystem\n(S'echo hello world'\ntR."
    with pytest.raises(UnSecureDataError):
        await cache.get("key")


async def test_no_value(cache):
    assert await cache.get("key") is None


async def test_replace_values(cache):
    await cache.set("key", "key")
    cache.store["replace"] = cache.store["key"]

    with pytest.raises(UnSecureDataError):
        await cache.get("replace")


async def test_pickle_error_value(cache):
    cache.store["key"] = cache.get_sign("key", b"no_pickle_data") + b"_" + b"no_pickle_data"
    assert await cache.get("key") is None


async def test_set_no_ser(cache):
    empty = object()
    await cache.set("key_e", empty)


async def test_no_import_dc(cache):
    @dataclasses.dataclass
    class TestNoImport:
        test: str

    with pytest.raises(AttributeError):
        await cache.set("key_i", TestNoImport(test="test"))


Schema = None


async def test_data_change(cache):
    global Schema

    @dataclasses.dataclass
    class Schema:
        test: str

    await cache.set("key", Schema("test"))

    @dataclasses.dataclass
    class Schema:
        name: str

    # Check without data change
    await cache.set("key2", Schema("test"))
    value = await cache.get("key2")
    assert value.name == "test"

    # IN key class of data have changed so it should be invalid
    value = await cache.get("key")
    assert value is None
