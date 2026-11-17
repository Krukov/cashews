import pytest

from cashews.backends.memory import Memory
from cashews.wrapper.backend_settings import BackendNotAvailableError, settings_url_parse


@pytest.mark.parametrize(
    ("url", "params"),
    (
        ("://", {"disable": True}),
        ("mem://", {}),
        (
            "mem://?size=10&check_interval=0.01",
            {"size": 10, "check_interval": 0.01},
        ),
    ),
)
def test_url(url, params):
    backend_class, _params, _ = settings_url_parse(url)
    assert backend_class is Memory
    assert params == _params


@pytest.mark.parametrize(
    ("url", "error"),
    (
        (
            "redis://localhost:9000/0",
            "Redis backend requires `redis` to be installed.",
        ),
        ("disk://", "Disk backend requires `diskcache` to be installed."),
    ),
)
def test_url_but_backend_dependency_is_not_installed(url, error):
    with pytest.raises(BackendNotAvailableError) as excinfo:
        settings_url_parse(url)

    assert str(excinfo.value) == error


@pytest.mark.redis
@pytest.mark.parametrize(
    ("url", "params"),
    (
        (
            "redis://localhost:9000/0",
            {"address": "redis://localhost:9000/0"},
        ),
        (
            "redis://password@localhost:9000",
            {"address": "redis://password@localhost:9000"},
        ),
        (
            "redis://localhost/0/?password=password",
            {
                "address": "redis://localhost/0/",
                "password": "password",
            },
        ),
        (
            "redis://localhost/0/?secret=secret&password=test&suppress=1&minsize=3&create_connection_timeout=0.1",  # noqa: E501
            {
                "address": "redis://localhost/0/",
                "secret": "secret",
                "password": "test",
                "suppress": True,
                "minsize": 3,
                "create_connection_timeout": 0.1,
            },
        ),
        (
            "redis://localhost:9000?",
            {"address": "redis://localhost:9000"},
        ),
        (
            "redis://localhost:9000/0?client_side=true&client_side_listen_timeout=0.1",
            {
                "address": "redis://localhost:9000/0",
                "client_side": True,
                "client_side_listen_timeout": 0.1,
            },
        ),
        (
            "redis://0.0.0.0:9000?cluster=true",
            {
                "address": "redis://0.0.0.0:9000",
                "cluster": True,
            },
        ),
    ),
)
def test_url_with_redis_as_backend(url, params):
    from cashews.backends.redis import Redis

    backend_class, _params, _ = settings_url_parse(url)
    assert isinstance(backend_class(**params), Redis)
    assert params == _params


@pytest.mark.redis
@pytest.mark.parametrize(
    ("url", "expected_sentinels", "expected_service", "expected_db"),
    (
        (
            "redis+sentinel://localhost:26379/mymaster/0",
            [("localhost", 26379)],
            "mymaster",
            0,
        ),
        (
            "redis+sentinel://host1:26379,host2:26379,host3:26379/mymaster/2",
            [("host1", 26379), ("host2", 26379), ("host3", 26379)],
            "mymaster",
            2,
        ),
        (
            "redis+sentinel://localhost:26379",
            [("localhost", 26379)],
            "mymaster",
            0,
        ),
    ),
)
def test_url_with_sentinel_as_backend(url, expected_sentinels, expected_service, expected_db):
    from cashews.backends.redis import Redis

    backend_class, params, _ = settings_url_parse(url)
    backend = backend_class(**params)
    assert isinstance(backend, Redis)
    assert backend._is_sentinel is True
    assert backend._sentinels == expected_sentinels
    assert backend._sentinel_service == expected_service
    assert backend._sentinel_db == expected_db


@pytest.mark.diskcache
@pytest.mark.parametrize(
    ("url", "params"),
    (
        ("disk://", {}),
        ("disk://?size_limit=1000", {"size_limit": 1000}),
        (
            "disk://?directory=/tmp/cache&timeout=1&shards=0",
            {"directory": "/tmp/cache", "timeout": 1, "shards": 0},
        ),
    ),
)
def test_url_with_diskcache_as_backend(url, params):
    from cashews.backends.diskcache import DiskCache

    backend_class, _params, _ = settings_url_parse(url)
    assert backend_class is DiskCache
    assert params == _params
