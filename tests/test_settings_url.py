import pytest

from cashews.backends.memory import Memory
from cashews.wrapper import BackendNotAvailable, settings_url_parse


@pytest.mark.parametrize(
    ("url", "params"),
    (
        ("://", {"backend": Memory, "disable": True}),
        ("mem://", {"backend": Memory}),
        (
            "mem://?size=10&check_interval=0.01",
            {"backend": Memory, "size": 10, "check_interval": 0.01},
        ),
    ),
)
def test_url(url, params):
    assert settings_url_parse(url) == params


@pytest.mark.parametrize(
    ("url", "error"),
    (
        (
            "redis://localhost:9000/0",
            "Redis backend requires `redis` (or `aioredis`) to be installed.",
        ),
        ("disk://", "Disk backend requires `diskcache` to be installed."),
    ),
)
def test_url_but_backend_dependency_is_not_installed(url, error):
    with pytest.raises(BackendNotAvailable) as excinfo:
        settings_url_parse(url)

    assert str(excinfo.value) == error


@pytest.mark.redis
@pytest.mark.parametrize(
    ("url", "params"),
    (
        (
            "redis://localhost:9000/0",
            {"backend": None, "address": "redis://localhost:9000/0"},
        ),
        (
            "redis://password@localhost:9000",
            {"backend": None, "address": "redis://password@localhost:9000"},
        ),
        (
            "redis://localhost/0/?password=password",
            {
                "backend": None,
                "address": "redis://localhost/0/",
                "password": "password",
            },
        ),
        (
            "redis://localhost/0/?hash_key=secret&password=test&safe=1&minsize=3&create_connection_timeout=0.1",
            {
                "backend": None,
                "address": "redis://localhost/0/",
                "hash_key": "secret",
                "password": "test",
                "safe": True,
                "minsize": 3,
                "create_connection_timeout": 0.1,
            },
        ),
        (
            "redis://localhost:9000?",
            {"backend": None, "address": "redis://localhost:9000"},
        ),
    ),
)
def test_url_with_redis_as_backend(url, params):
    from cashews.backends.redis import Redis

    params["backend"] = Redis
    assert settings_url_parse(url) == params


@pytest.mark.diskcache
@pytest.mark.parametrize(
    ("url", "params"),
    (
        ("disk://", {"backend": None}),
        ("disk://?size_limit=1000", {"backend": None, "size_limit": 1000}),
        (
            "disk://?directory=/tmp/cache&timeout=1&shards=0",
            {"backend": None, "directory": "/tmp/cache", "timeout": 1, "shards": 0},
        ),
    ),
)
def test_url_with_diskcache_as_backend(url, params):
    from cashews.backends.diskcache import DiskCache

    params["backend"] = DiskCache
    assert settings_url_parse(url) == params
