import pytest

from mergify_engine import config


def test_github_api_url(
    original_environment_variables: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    conf = config.load()
    assert conf["GITHUB_REST_API_URL"] == "https://api.github.com"
    assert conf["GITHUB_GRAPHQL_API_URL"] == "https://api.github.com/graphql"

    monkeypatch.setenv("MERGIFYENGINE_GITHUB_URL", "https://onprem.example.com")
    conf = config.load()
    assert conf["GITHUB_REST_API_URL"] == "https://onprem.example.com/api/v3"
    assert conf["GITHUB_GRAPHQL_API_URL"] == "https://onprem.example.com/api/graphql"

    monkeypatch.setenv("MERGIFYENGINE_GITHUB_URL", "https://onprem.example.com/")
    conf = config.load()
    assert conf["GITHUB_REST_API_URL"] == "https://onprem.example.com/api/v3"
    assert conf["GITHUB_GRAPHQL_API_URL"] == "https://onprem.example.com/api/graphql"

    monkeypatch.setenv("MERGIFYENGINE_GITHUB_URL", "https://github.com/")
    conf = config.load()
    assert conf["GITHUB_REST_API_URL"] == "https://api.github.com"
    assert conf["GITHUB_GRAPHQL_API_URL"] == "https://api.github.com/graphql"


def test_redis_onpremise_legacy(
    original_environment_variables: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MERGIFYENGINE_DEFAULT_REDIS_URL")
    monkeypatch.setenv("MERGIFYENGINE_STORAGE_URL", "rediss://redis.example.com:1234")
    conf = config.load()
    assert conf["STREAM_URL"] == "rediss://redis.example.com:1234"
    assert conf["QUEUE_URL"] == "rediss://redis.example.com:1234"
    assert conf["LEGACY_CACHE_URL"] == "rediss://redis.example.com:1234"
    assert conf["TEAM_MEMBERS_CACHE_URL"] == "rediss://redis.example.com:1234?db=5"
    assert conf["TEAM_PERMISSIONS_CACHE_URL"] == "rediss://redis.example.com:1234?db=6"
    assert conf["USER_PERMISSIONS_CACHE_URL"] == "rediss://redis.example.com:1234?db=7"


def test_redis_saas_current(
    original_environment_variables: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "MERGIFYENGINE_DEFAULT_REDIS_URL", "rediss://redis.example.com:1234"
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_STORAGE_URL",
        "rediss://redis-legacy-cache.example.com:1234?db=2",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_STREAM_URL",
        "rediss://redis-stream.example.com:1234?db=3",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_QUEUE_URL", "rediss://redis-queue.example.com:1234?db=4"
    )
    conf = config.load()
    assert (
        conf["LEGACY_CACHE_URL"] == "rediss://redis-legacy-cache.example.com:1234?db=2"
    )
    assert conf["STREAM_URL"] == "rediss://redis-stream.example.com:1234?db=3"
    assert conf["QUEUE_URL"] == "rediss://redis-queue.example.com:1234?db=4"
    assert conf["TEAM_MEMBERS_CACHE_URL"] == "rediss://redis.example.com:1234?db=5"
    assert conf["TEAM_PERMISSIONS_CACHE_URL"] == "rediss://redis.example.com:1234?db=6"
    assert conf["USER_PERMISSIONS_CACHE_URL"] == "rediss://redis.example.com:1234?db=7"


def test_redis_default(
    original_environment_variables: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "MERGIFYENGINE_DEFAULT_REDIS_URL", "rediss://redis.example.com:1234"
    )
    conf = config.load()
    assert conf["LEGACY_CACHE_URL"] == "rediss://redis.example.com:1234?db=2"
    assert conf["STREAM_URL"] == "rediss://redis.example.com:1234?db=3"
    assert conf["QUEUE_URL"] == "rediss://redis.example.com:1234?db=4"
    assert conf["TEAM_MEMBERS_CACHE_URL"] == "rediss://redis.example.com:1234?db=5"
    assert conf["TEAM_PERMISSIONS_CACHE_URL"] == "rediss://redis.example.com:1234?db=6"
    assert conf["USER_PERMISSIONS_CACHE_URL"] == "rediss://redis.example.com:1234?db=7"


def test_redis_all_set(
    original_environment_variables: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "MERGIFYENGINE_DEFAULT_REDIS_URL",
        "rediss://redis-default.example.com:1234",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_LEGACY_CACHE_URL",
        "rediss://redis-legacy-cache.example.com:1234",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_STREAM_URL",
        "rediss://redis-stream.example.com:1234",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_QUEUE_URL", "rediss://redis-queue.example.com:1234"
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_TEAM_MEMBERS_CACHE_URL",
        "rediss://redis-team-members.example.com:1234",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_TEAM_PERMISSIONS_CACHE_URL",
        "rediss://redis-team-perm.example.com:1234",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_USER_PERMISSIONS_CACHE_URL",
        "rediss://redis-user-perm.example.com:1234",
    )
    conf = config.load()
    assert conf["LEGACY_CACHE_URL"] == "rediss://redis-legacy-cache.example.com:1234"
    assert conf["STREAM_URL"] == "rediss://redis-stream.example.com:1234"
    assert conf["QUEUE_URL"] == "rediss://redis-queue.example.com:1234"
    assert (
        conf["TEAM_MEMBERS_CACHE_URL"] == "rediss://redis-team-members.example.com:1234"
    )
    assert (
        conf["TEAM_PERMISSIONS_CACHE_URL"]
        == "rediss://redis-team-perm.example.com:1234"
    )
    assert (
        conf["USER_PERMISSIONS_CACHE_URL"]
        == "rediss://redis-user-perm.example.com:1234"
    )
