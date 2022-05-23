from datetime import timedelta

import pytest

from cashews.formatter import default_formatter, get_template_and_func_for, get_template_for_key, register_template
from cashews.key import get_cache_key, get_cache_key_template, ttl_to_seconds


async def func1(a):
    ...


async def func2(a, *, k=None, **kwargs):
    ...


async def func3(a, *, k="test"):
    ...


class Klass:
    data = {"test": 1}

    def method(self, a, k=None):
        ...


TEPLATE_FUNC1 = "func1:{a}"
TEPLATE_FUNC2 = "func2:{k}:user:{user}"
TEPLATE_FUNC3 = "func3:{k:len}"


@default_formatter.type_format(Klass)
def _some(value: Klass):
    return "hoho"


@pytest.mark.parametrize(
    ("key", "template", "func"),
    (
        ("func1:test", TEPLATE_FUNC1.format(a="*"), func1),
        ("func1:", TEPLATE_FUNC1.format(a="*"), func1),
        ("prefix:func1:test", TEPLATE_FUNC1.format(a="*"), func1),
        ("func2:-:user:1", TEPLATE_FUNC2.format(k="*", user="*"), func2),
        ("func3:2", "func3:*", func3),
        ("func:1", None, None),
        ("prefix:func2:test:user:1:1", None, None),
        ("func2:user:1", None, None),
        ("func2:user:1", None, None),
    ),
)
def test_detect_template_and_func_by_key(key, template, func):
    register_template(func1, TEPLATE_FUNC1)
    register_template(func2, TEPLATE_FUNC2)
    register_template(func3, TEPLATE_FUNC3)
    _template, _func = get_template_and_func_for(key)
    assert _template == template
    assert _func == ((func.__module__ or "", func.__name__) if func else None)


@pytest.mark.parametrize(
    ("key", "template", "params"),
    (
        ("func1:test", TEPLATE_FUNC1, {"a": "test"}),
        ("func1:", TEPLATE_FUNC1, {"a": ""}),
        ("prefix:func1:test", TEPLATE_FUNC1, {"a": "test"}),
        ("func2:-:user:1", TEPLATE_FUNC2, {"k": "-", "user": "1"}),
        ("func3:2", TEPLATE_FUNC3, {"k": "2"}),
        ("func:1", None, None),
        ("prefix:func2:test:user:1:1", None, None),
        ("func2:user:1", None, None),
        ("func2:user:1", None, None),
    ),
)
def test_detect_template_by_key(key, template, params):
    register_template(func1, TEPLATE_FUNC1)
    register_template(func2, TEPLATE_FUNC2)
    register_template(func3, TEPLATE_FUNC3)

    _template, _params = get_template_for_key(key)
    assert _template == template
    assert _params == params


def test_cache_func_key_dict():
    async def func(user):
        ...

    obj = type("user", (), {"name": "test", "one": True})()

    assert get_cache_key(func, template="test_key:{user.name}", args=(obj,)) == "test_key:test"

    obj = type("user", (), {"name": "new", "one": True})()
    assert get_cache_key(func, template="test_key:{user.name}", args=(obj,)) == "test_key:new"


@pytest.mark.parametrize(
    ("args", "kwargs", "template", "key"),
    (
        (
            ("A1", "a2", "a3"),
            {"kwarg1": "k1", "kwarg3": True},
            "{arg1}:{kwarg1}-{kwarg3}",
            "A1:k1-true",
        ),
        (
            ("A1", "a2", "a3"),
            {"kwarg1": "k1", "kwarg0": True},
            None,
            "tests.test_key:func:arg1:A1:arg2:a2:a3:kwarg1:k1:kwarg2:true:kwarg0:true",
        ),
        (
            ("A1", "a2", "b", True, 1, 1.2, ("a", 1, 1.2)),
            {"kwarg1": "k1", "e": True, "d": 1},
            "{__args__}:{__kwargs__}",
            "b:true:1:1.2:a:1:1.2:d:1:e:true",
        ),
        (
            (b"a1", "a2", "a3"),
            None,
            "{arg1}-{kwarg1}-{kwarg3}",
            "a1--",
        ),
        (
            (True, "a2", "a3"),
            None,
            "{arg1}-{kwarg1}-{kwarg3}",
            "true--",
        ),
        (
            (b"\x16F\xbd\xb0\xcf\xcdN\xd7Y)\xfa\x1d\x96\xb1u\x81", ""),
            None,
            "{arg1}",
            "1646bdb0cfcd4ed75929fa1d96b17581",
        ),
        (
            (Klass(), ""),
            None,
            "{arg1}",
            "hoho",
        ),
        (
            ("a1",),
            {"arg2": 2, "kwarg1": "k1"},
            "{arg2}-{kwarg1}-{kwarg3}",
            "2-k1-",
        ),
        (
            ("a1",),
            {"arg2": 2, "kwarg1": "k1", "kwarg3": "k3"},
            "{arg2}:{kwarg1}:{kwarg3}",
            "2:k1:k3",
        ),
        (
            ("a1",),
            {"kwarg1": "K1", "arg2": 2},
            "{arg2}:{kwarg1}:{kwarg3}",
            "2:K1:",
        ),
        (("a1", "a2"), {"kwarg1": "test"}, "{kwarg1:len}", "4"),
        (
            ("a1", "a2"),
            {"user": type("user", (), {"name": "test"})()},
            "{user.name:len}",
            "4",
        ),
        (
            ("a1", "a2"),
            {"kwarg1": "test"},
            "{kwarg1:hash}",
            "098f6bcd4621d373cade4e832627b4f6",
        ),
        (
            ("a1", "a2"),
            {"kwarg1": "test"},
            "{kwarg1:hash(md5)}",
            "098f6bcd4621d373cade4e832627b4f6",
        ),
        (
            ("a1", "a2"),
            {"kwarg1": "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyIjoidGVzdCJ9.n75bSCIEuSemX5iop0sCF3HmXwMQWF1zI6TzckzHUKY"},
            "{kwarg1:jwt(user)}",
            "test",
        ),
    ),
)
def test_cache_key_args_kwargs(args, kwargs, template, key):
    async def func(arg1, arg2, *args, kwarg1=None, kwarg2=b"true", **kwargs):
        ...

    assert get_cache_key(func, template, args=args, kwargs=kwargs) == key


@pytest.mark.parametrize(
    ("func", "key", "template"),
    (
        (func1, None, "tests.test_key:func1:a:{a}"),
        (func2, None, "tests.test_key:func2:a:{a}:k:{k}:{__kwargs__}"),
        (func3, None, "tests.test_key:func3:a:{a}:k:{k}"),
        (Klass.method, None, "tests.test_key:Klass.method:self:{self}:a:{a}:k:{k}"),
        (Klass.method, "key:{k}:{self.data.test}", "key:{k}:{self.data.test}"),
        (func2, "key:{k}", "key:{k}"),
        (func3, "key:{k:len}:{k:hash(md5)}", "key:{k:len}:{k:hash(md5)}"),
    ),
)
def test_get_key_template(func, key, template):
    assert get_cache_key_template(func, key) == template


def test_get_key_template_error():
    with pytest.raises(ValueError) as exc:
        get_cache_key_template(func1, "key:{wrong_key}:{a}")
    exc.match("wrong_key")


@pytest.mark.parametrize(
    ("ttl", "expect"),
    (
        (timedelta(seconds=10), 10),
        (lambda: timedelta(hours=1), 60 * 60),
        (10, 10),
        (100.1, 100.1),
        ("10s", 10),
        ("1m10s", 60 + 10),
        ("10m1s", 60 * 10 + 1),
        ("1", 1),
        ("80", 80),
        (lambda: "1h", 60 * 60),
    ),
)
def test_ttl_to_seconds(ttl, expect):
    assert ttl_to_seconds(ttl) == expect
