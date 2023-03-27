import dataclasses
from collections import namedtuple
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio
from hypothesis import example, given, settings
from hypothesis import strategies as st

from cashews.backends.memory import Memory
from cashews.serialize import UnSecureDataError

pytestmark = pytest.mark.asyncio


@dataclasses.dataclass()
class TestDC:
    test: str
    _: int


NT = namedtuple("NT", ("test", "name"), defaults=[False, "test"])


@pytest_asyncio.fixture(
    name="cache",
    params=[
        "default_md5",
        "default_sum",
        "default_sha256",
        pytest.param("redis_md5", marks=pytest.mark.redis),
        pytest.param("redis_sum", marks=pytest.mark.redis),
        pytest.param("dill_sum", marks=pytest.mark.integration),
        pytest.param("sqlalchemy_sha1", marks=pytest.mark.integration),
    ],
)
async def _cache(request, redis_dsn):
    pickle_type, digestmod = request.param.split("_")
    if pickle_type == "redis":
        from cashews.backends.redis import Redis

        redis = Redis(redis_dsn, hash_key="test", safe=False, digestmod=digestmod)
        await redis.init()
        await redis.clear()
        return redis

    return Memory(hash_key=b"test", digestmod=digestmod, pickle_type=pickle_type)


@pytest.mark.parametrize(
    "value",
    (
        "test",
        b"test",
        "1_1",
        b"1_1",
        b"1_1_1",
        "1",
        b"0",
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
    ("value", "encoded"),
    (
        ("test", b"md5:2182de23a749c70144594da72f8cedc2_\x80\x05\x95\b\x00\x00\x00\x00\x00\x00\x00\x8c\x04test\x94."),
        (b"test", b"md5:aa47dd7b9ec8a973cdea43bce56cd34c_bytes:test"),
        (0, b"0"),
        (1.234, b"sum:915_\x80\x05\x95\n\x00\x00\x00\x00\x00\x00\x00G?\xf3\xbev\xc8\xb49X."),
        (
            Decimal("1.001"),
            b"sum:117b_\x80\x05\x95#\x00\x00\x00\x00\x00\x00\x00\x8c\adecimal\x94\x8c\aDecimal\x94\x93\x94\x8c\x05"
            b"1.001\x94\x85\x94R\x94.",
        ),
        (True, b"md5:b5d04289f61dd9a7cb8856222e64589a_\x80\x05\x88."),
        (None, b"sum:40a_\x80\x05N."),
        ("1", b"sum:5a8_\x80\x05\x95\x05\x00\x00\x00\x00\x00\x00\x00\x8c\x011\x94."),
        ({"hi": True}, b"sum:85a_\x80\x05\x95\n\x00\x00\x00\x00\x00\x00\x00}\x94\x8c\x02hi\x94\x88s."),
        (
            TestDC(test="test", _=1),
            b"sum:1e3f_\x80\x05\x95B\x00\x00\x00\x00\x00\x00\x00\x8c\x1ctests.test_pickle_serializer\x94\x8c\x06"
            b"TestDC\x94\x93\x94)\x81\x94}\x94(\x8c\x04test\x94h\x05\x8c\x01_\x94K\x01ub.",
        ),
        (
            [NT(name="test", test="lol"), 1, timedelta(days=10), TestDC(test="test", _=1)],
            b"sum:360b_\x80\x05\x95\x80\x00\x00\x00\x00\x00\x00\x00]\x94(\x8c\x1ctests.test_pickle_serializer"
            b"\x94\x8c\x02NT\x94\x93\x94\x8c\x03lol\x94\x8c\x04test\x94\x86\x94\x81\x94K\x01\x8c\bdatetime"
            b"\x94\x8c\ttimedelta\x94\x93\x94K\nK\x00K\x00\x87\x94R\x94h\x01\x8c\x06TestDC"
            b"\x94\x93\x94)\x81\x94}\x94(h\x05h\x05\x8c\x01_\x94K\x01ube.",
        ),
        (b"1", b"md5:fa81eb7dc6d56848cb9b3dab70437927_bytes:1"),
        (1, b"1"),
        ("", b"sum:575_\x80\x05\x95\x04\x00\x00\x00\x00\x00\x00\x00\x8c\x00\x94."),
    ),
)
async def test_serialize_backward(value, encoded, cache):
    await cache.set_raw("key", encoded)
    assert await cache.get("key") == value


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
    assert await cache.get("key", default="default") == "default"


async def test_replace_values(cache):
    await cache.set("key", "key")
    await cache.set_raw("replace", await cache.get_raw("key"))

    with pytest.raises(UnSecureDataError):
        await cache.get("replace")


async def test_pickle_error_value(cache):
    await cache.set_raw(
        "key",
        cache._serializer._signer.sign("key", b"no_pickle_data"),
    )
    assert await cache.get("key", default="default") == "default"


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
    cache = Memory()
    await cache.set(key, value)
    assert await cache.get(key) == value


async def test_cache_from_hash_to_no_hash():
    val = Decimal("10.2")
    cache = Memory(hash_key="test")
    await cache.set("key", val)

    assert await cache.get("key") == val
    cache_no_hash = Memory()
    cache_no_hash.store = cache.store
    assert await cache_no_hash.get("key", default="default") == "default"


async def test_cache_from_no_hash_to_hash():
    val = Decimal("10.2")
    cache = Memory()
    await cache.set("key", val)

    assert await cache.get("key") == val
    cache_hash = Memory(hash_key="test")
    cache_hash.store = cache.store

    assert await cache_hash.get("key") == val
