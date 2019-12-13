from operator import attrgetter

import pytest
from cashews.key import get_cache_key, get_cache_key_template


def test_cache_func_key_dict():
    async def func(user):
        ...

    obj = type("user", (), {"name": b"test", "one": True})()

    assert get_cache_key(func, args=(obj,), func_args={"user": attrgetter("name")}) == "tests.test_key:func:user:test"

    obj.name = b"new"
    assert get_cache_key(func, args=(obj,), func_args={"user": attrgetter("name")}) == "tests.test_key:func:user:new"


@pytest.mark.parametrize(
    ("args", "kwargs", "func_args", "key"),
    (
        (
            ("a1", "a2", "a3"),
            {"kwarg1": "k1", "kwarg3": "k3"},
            ("arg1", "kwarg1", "kwarg3"),
            "tests.test_key:func:arg1:a1:kwarg1:k1:kwarg3:k3",
        ),
        (
            ("a1", "a2", "a3"),
            {"kwarg1": "k1", "kwarg3": "k3"},
            None,
            "tests.test_key:func:arg1:a1:arg2:a2:kwarg1:k1:kwarg2:true:kwarg3:k3",
        ),
        (("a1", "a2", "a3"), None, ("arg1", "kwarg1", "kwarg3"), "tests.test_key:func:arg1:a1:kwarg1:"),
        (("a1",), {"arg2": 2, "kwarg1": "k1"}, ("arg2", "kwarg1", "kwarg3"), "tests.test_key:func:arg2:2:kwarg1:k1"),
        (("a1",), {"kwarg1": "k1", "arg2": 2}, ("arg2", "kwarg1", "kwarg3"), "tests.test_key:func:arg2:2:kwarg1:k1"),
    ),
)
def test_cache_key_args_kwargs(args, kwargs, func_args, key):
    async def func(arg1, arg2, *args, kwarg1=None, kwarg2=b"true", **kwargs):
        ...

    assert get_cache_key(func, args=args, kwargs=kwargs, func_args=func_args) == key


@pytest.mark.parametrize(
    ("args", "func_args", "key", "result"),
    (
        (("a", "k"), ("a", "k"), "key:{a}-{k}", "key:a-k"),
        (("a",), ("a", "k"), "key:{a}-{k}", "key:a-none"),
        (("a",), ("a", "k"), "key:{a}-", "key:a-"),
    ),
)
def test_cache_key_key(args, func_args, key, result):
    async def func(a="a1", k=None):
        ...

    assert get_cache_key(func, args=args, func_args=func_args, key=key) == result


async def func1(a):
    ...


async def func2(a, k=None, **kwargs):
    ...


@pytest.mark.parametrize(
    ("func", "func_args", "key", "template"),
    (
        (func1, None, None, "tests.test_key:func1:a:{a}"),
        (func2, None, None, "tests.test_key:func2:a:{a}:k:{k}"),
        (func2, ("a",), None, "tests.test_key:func2:a:{a}"),
        (func2, ("k",), None, "tests.test_key:func2:k:{k}"),
        (func2, ("k", "test"), None, "tests.test_key:func2:k:{k}:test:{test}"),
        (func2, {"k": ""}, None, "tests.test_key:func2:k:{k}"),
        (func2, None, "key", "key"),
    ),
)
def test_get_key_template(func, func_args, key, template):
    assert get_cache_key_template(func, func_args, key) == template
