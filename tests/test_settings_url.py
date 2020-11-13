import pytest
from cashews.backends.memory import Memory
from cashews.backends.redis import Redis
from cashews.wrapper import settings_url_parse


@pytest.mark.parametrize(
    ("url", "params"),
    (
        ("://", {"backend": Memory, "disable": True}),
        ("mem://", {"backend": Memory}),
        ("mem://?size=10&check_interval=0.01", {"backend": Memory, "size": 10, "check_interval": 0.01}),
        ("redis://localhost:9000/0", {"backend": Redis, "address": "redis://localhost:9000/0"}),
        ("redis://password@localhost:9000", {"backend": Redis, "address": "redis://password@localhost:9000"}),
        (
            "redis://localhost/0/?password=password",
            {"backend": Redis, "address": "redis://localhost/0/", "password": "password"},
        ),
        (
            "redis://localhost/0/?hash_key=secret&password=test&safe=1&minsize=3&create_connection_timeout=0.1",
            {
                "backend": Redis,
                "address": "redis://localhost/0/",
                "hash_key": "secret",
                "password": "test",
                "safe": True,
                "minsize": 3,
                "create_connection_timeout": 0.1,
            },
        ),
        ("redis://localhost:9000?", {"backend": Redis, "address": "redis://localhost:9000"}),
    ),
)
def test_url(url, params):
    assert settings_url_parse(url) == params
