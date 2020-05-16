from operator import attrgetter

import pytest
from cashews.key import get_cache_key, get_cache_key_template


def test_cache_func_key_dict():
    async def func(user):
        ...

    obj = type("user", (), {"name": "test", "one": True})()

    assert get_cache_key(func, template="test_key:{user.name}", args=(obj,)) == "test_key:test"

    obj.name = "new"
    assert get_cache_key(func, template="test_key:{user.name}", args=(obj,)) == "test_key:new"


@pytest.mark.parametrize(
    ("args", "kwargs", "template", "key"),
    (
        (("a1", "a2", "a3"), {"kwarg1": "k1", "kwarg3": "k3"}, "{arg1}:{kwarg1}-{kwarg3}", "a1:k1-k3",),
        (
            ("a1", "a2", "a3"),
            {"kwarg1": "k1", "kwarg3": "k3"},
            None,
            "tests.test_key:func:arg1:a1:arg2:a2:kwarg1:k1:kwarg2:true",
        ),
        (("a1", "a2", "a3"), None, "{arg1}-{kwarg1}-{kwarg3}", "a1--",),
        (("a1",), {"arg2": 2, "kwarg1": "k1"}, "{arg2}-{kwarg1}-{kwarg3}", "2-k1-",),
        (("a1",), {"arg2": 2, "kwarg1": "k1", "kwarg3": "k3"}, "{arg2}:{kwarg1}:{kwarg3}", "2:k1:k3",),
        (("a1",), {"kwarg1": "k1", "arg2": 2}, "{arg2}:{kwarg1}:{kwarg3}", "2:k1:",),
    ),
)
def test_cache_key_args_kwargs(args, kwargs, template, key):
    async def func(arg1, arg2, *args, kwarg1=None, kwarg2=b"true", **kwargs):
        ...

    assert get_cache_key(func, template, args=args, kwargs=kwargs) == key


@pytest.mark.parametrize(
    ("args", "func_args", "result"),
    ((("a", "k"), ("a", "k"), "tests.test_key:func:a:a:k:k"), (("a",), ("a", "k"), "tests.test_key:func:a:a"),),
)
def atest_cache_key_key(args, func_args, result):
    async def func(a="a1", k=None):
        ...

    assert get_cache_key(func, args=args, func_args=func_args) == result


async def func1(a):
    ...


async def func2(a, k=None, **kwargs):
    ...


@pytest.mark.parametrize(
    ("func", "key", "template"),
    (
        (func1, None, "tests.test_key:func1:a:{a}"),
        (func2, None, "tests.test_key:func2:a:{a}:k:{k}"),
        (func2, "key:{test}", "key:{test}"),
    ),
)
def test_get_key_template(func, key, template):
    assert get_cache_key_template(func, key) == template
