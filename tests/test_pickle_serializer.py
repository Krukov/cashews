import dataclasses
from collections import namedtuple
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio
from hypothesis import example, given, settings
from hypothesis import strategies as st

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

    async def get(self, key: str, default=None, **kwargs):
        return self.store.get(key, default)

    async def delete(self, *keys):
        for key in keys:
            del self.store[key]

    async def get_many(self, *keys, default=None):
        return [self.store.get(key, default) for key in keys]

    async def set_many(self, pairs, expire=None):
        self.store.update(pairs)


class Cache(PickleSerializerMixin, DummyCache):
    pass


@pytest_asyncio.fixture(
    name="cache",
    params=[
        "default_md5",
        "default_sum",
        "default_sha256",
        pytest.param("redis_md5", marks=pytest.mark.redis),
        pytest.param("dill_sum", marks=pytest.mark.integration),
        pytest.param("sqlalchemy_sha1", marks=pytest.mark.integration),
    ],
)
async def _cache(request, redis_dsn):
    pickle_type, digestmod = request.param.split("_")
    if pickle_type == "redis":
        from cashews.backends.redis import Redis

        redis = Redis(redis_dsn, hash_key=b"test", safe=False, digestmod=digestmod)
        await redis.init()
        await redis.clear()
        return redis
    return Cache(hash_key="test", digestmod=digestmod, pickle_type=pickle_type)


@pytest.mark.parametrize(
    "value",
    (
        "test",
        b"test",
        "1_1",
        b"1_1",
        b"1_1_1",
        0,
        1,
        2,
        1.234,
        Decimal("1.001"),
        True,
        False,
        "",
        None,
        {"hi": True},
        TestDC(test="test", _=1),
    ),
)
async def test_serialize_simple_value(value, cache):
    await cache.set("key", value)
    await cache.set_many({"key1": value, "key2": value})
    assert await cache.get("key") == value
    assert await cache.get("key2") == value
    assert await cache.get_many("key1", "key2") == (value, value)


@pytest.mark.parametrize("value", (NT(name="test", test="lol"), NT()))
async def test_serialize_collections_value(value, cache):
    await cache.set("key", value)
    assert await cache.get("key") == value
    assert (await cache.get("key")).name == "test"


@pytest.mark.parametrize(
    "value",
    (
        datetime(year=2000, month=1, day=10, hour=10),
        timedelta(days=10),
        date(year=2020, month=12, day=31),
    ),
)
async def test_serialize_dates_value(value, cache):
    await cache.set("key", value)
    assert await cache.get("key") == value


@pytest.mark.parametrize(
    "value",
    (
        ["test", "to"],
        (TestDC("hay", _=2),),
        {"test"},
        [(1, 2), (3, 4)],
        {1, 2, 5},
        [{"test": True}],
    ),
)
async def test_serialize_array_value(value, cache):
    await cache.set("key", value)
    assert list(await cache.get("key")) == list(value)
    assert type(await cache.get("key")) == type(value)


@pytest.mark.parametrize(
    "value",
    (
        ["test", b"to"],
        (TestDC("hay", _=2), Decimal("10.1")),
        {"test", 1},
        [],
        [{1: True}, Decimal("0.1")],
    ),
)
async def test_serialize_array_diff_value(value, cache):
    await cache.set("key", value)
    assert list(await cache.get("key")) == list(value)
    assert type(await cache.get("key")) == type(value)


@pytest.mark.parametrize(
    "value",
    (
        b"_cos\nsystem\n(S'echo hello world'\ntR.",
        b":_cos\nsystem\n(S'echo hello world'\ntR.",
        b"md5:_cos\nsystem\n(S'echo hello world'\ntR.",
        b"sha1:_cos\nsystem\n(S'echo hello world'\ntR.",
        b"__cos\nsystem\n(S'echo hello world'\ntR.",
    ),
)
async def test_unsecure_value(value, cache):
    await cache.set_raw("key", value)
    with pytest.raises(UnSecureDataError):
        assert not await cache.get("key")


async def test_unsecure_value_many(cache):
    await cache.set_raw("key", b"_cos\nsystem\n(S'echo hello world'\ntR.")
    with pytest.raises(UnSecureDataError):
        await cache.get_many("key")


async def test_no_value(cache):
    assert await cache.get("key") is None


async def test_replace_values(cache):
    await cache.set("key", "key")
    await cache.set_raw("replace", await cache.get_raw("key"))

    with pytest.raises(UnSecureDataError):
        await cache.get("replace")


async def test_pickle_error_value(cache):
    await cache.set_raw(
        "key",
        cache._gen_sign("key", b"no_pickle_data", cache._digestmod) + b"_" + b"no_pickle_data",
    )
    assert await cache.get("key") is None


async def test_set_no_ser(cache):
    empty = object()
    await cache.set("key_e", empty)


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


async def test_get_set_raw(cache):
    await cache.set_raw("key", b"test")
    assert await cache.get_raw("key") == b"test"


@given(key=st.text(), value=st.characters())
@settings(max_examples=500)
@example(key="_key:_!@#$%^&*()", value='_value:_!@_#$%^&:*(?)".,4й')
async def test_no_hash(key, value):
    cache = Cache()
    await cache.set(key, value)
    assert await cache.get(key) == value


async def test_cache_from_hash_to_no_hash():
    val = Decimal("10.2")
    cache = Cache(hash_key="test")
    await cache.set("key", val)

    assert await cache.get("key") == val
    cache_no_hash = Cache()
    cache_no_hash.store = cache.store
    assert await cache_no_hash.get("key") == val


async def test_cache_from_no_hash_to_hash():
    val = Decimal("10.2")
    cache = Cache()
    await cache.set("key", val)

    assert await cache.get("key") == val
    cache_hash = Cache(hash_key="test")
    cache_hash.store = cache.store

    assert await cache_hash.get("key") is None
