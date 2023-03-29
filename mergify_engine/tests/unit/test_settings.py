import os

import pydantic
import pytest

from mergify_engine import config
from mergify_engine.config import urls


@pytest.fixture
def unset_testing_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config.EngineSettings.Config, "env_file", None)
    for env in os.environ:
        if env.startswith("MERGIFYENGINE"):
            monkeypatch.delenv(env)


def test_defaults(
    original_environment_variables: None,
    unset_testing_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # defaults (if not mandatory)
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_WEBHOOK_SECRET", "secret")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_APP_ID", "12345")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_PRIVATE_KEY", "aGVsbG8gd29ybGQ=")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_OAUTH_CLIENT_ID", "Iv1.XXXXXX")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_OAUTH_CLIENT_SECRET", "secret")
    conf = config.EngineSettings()
    assert str(conf.DATABASE_URL) == "postgresql+psycopg://localhost:5432"
    assert conf.DATABASE_URL.geturl() == "postgresql+psycopg://localhost:5432"
    assert conf.DATABASE_POOL_SIZES == {"web": 55, "worker": 15}
    assert conf.GITHUB_URL == "https://github.com"
    assert conf.GITHUB_REST_API_URL == "https://api.github.com"
    assert conf.GITHUB_GRAPHQL_API_URL == "https://api.github.com/graphql"
    assert conf.GITHUB_APP_ID == 12345
    assert conf.GITHUB_PRIVATE_KEY.get_secret_value() == "hello world"
    assert conf.GITHUB_OAUTH_CLIENT_ID == "Iv1.XXXXXX"
    assert conf.GITHUB_OAUTH_CLIENT_SECRET.get_secret_value() == "secret"
    assert conf.GITHUB_WEBHOOK_SECRET.get_secret_value() == "secret"
    assert conf.GITHUB_WEBHOOK_SECRET_PRE_ROTATION is None
    assert conf.GITHUB_WEBHOOK_FORWARD_EVENT_TYPES == []
    assert conf.GITHUB_WEBHOOK_FORWARD_URL is None
    assert conf.DASHBOARD_UI_STATIC_FILES_DIRECTORY is None
    assert conf.DASHBOARD_UI_FRONT_URL == "http://localhost:8802"
    assert conf.DASHBOARD_UI_FEATURES == []
    assert conf.DASHBOARD_UI_SESSION_EXPIRATION_HOURS == 24
    assert conf.DASHBOARD_UI_DATADOG_CLIENT_TOKEN is None
    assert conf.DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO == []


def test_all_sets(
    original_environment_variables: None,
    unset_testing_env: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    monkeypatch.setattr(config.EngineSettings.Config, "env_file", None)

    tmpdir = tmp_path_factory.mktemp("whatever")

    monkeypatch.setenv("MERGIFYENGINE_GITHUB_URL", "https://my-ghes.example.com")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_APP_ID", "12345")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_PRIVATE_KEY", "aGVsbG8gd29ybGQ=")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_OAUTH_CLIENT_ID", "Iv1.XXXXXX")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_OAUTH_CLIENT_SECRET", "secret")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_WEBHOOK_SECRET", "secret2")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_WEBHOOK_FORWARD_EVENT_TYPES", "foo,bar,yo")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_WEBHOOK_SECRET_PRE_ROTATION", "secret3")
    monkeypatch.setenv(
        "MERGIFYENGINE_GITHUB_WEBHOOK_FORWARD_URL", "https://sub.example.com/events"
    )
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_POOL_SIZES", "web:2,worker:3,foobar:6")
    monkeypatch.setenv(
        "MERGIFYENGINE_DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO", "1234,5432"
    )
    monkeypatch.setenv("MERGIFYENGINE_DASHBOARD_UI_STATIC_FILES_DIRECTORY", str(tmpdir))
    monkeypatch.setenv("MERGIFYENGINE_DASHBOARD_UI_DATADOG_CLIENT_TOKEN", "no-secret")
    monkeypatch.setenv("MERGIFYENGINE_DASHBOARD_UI_SESSION_EXPIRATION_HOURS", "100")
    monkeypatch.setenv(
        "MERGIFYENGINE_DASHBOARD_UI_FEATURES",
        "subscriptions,applications,intercom,statuspage",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_DASHBOARD_UI_FRONT_URL",
        "https://dashboard.mergify.com",
    )

    conf = config.EngineSettings()
    assert conf.GITHUB_URL == "https://my-ghes.example.com"
    assert conf.GITHUB_REST_API_URL == "https://my-ghes.example.com/api/v3"
    assert conf.GITHUB_GRAPHQL_API_URL == "https://my-ghes.example.com/api/graphql"
    assert conf.GITHUB_APP_ID == 12345
    assert conf.GITHUB_PRIVATE_KEY.get_secret_value() == "hello world"
    assert conf.GITHUB_OAUTH_CLIENT_ID == "Iv1.XXXXXX"
    assert conf.GITHUB_OAUTH_CLIENT_SECRET.get_secret_value() == "secret"
    assert conf.GITHUB_WEBHOOK_SECRET.get_secret_value() == "secret2"
    assert conf.GITHUB_WEBHOOK_SECRET_PRE_ROTATION is not None
    assert conf.GITHUB_WEBHOOK_SECRET_PRE_ROTATION.get_secret_value() == "secret3"
    assert conf.GITHUB_WEBHOOK_FORWARD_EVENT_TYPES == ["foo", "bar", "yo"]
    assert conf.GITHUB_WEBHOOK_FORWARD_URL == "https://sub.example.com/events"
    assert conf.DATABASE_POOL_SIZES == {"web": 2, "worker": 3, "foobar": 6}
    assert conf.DASHBOARD_UI_STATIC_FILES_DIRECTORY == tmpdir
    assert conf.DASHBOARD_UI_FRONT_URL == "https://dashboard.mergify.com"
    assert conf.DASHBOARD_UI_FEATURES == [
        "subscriptions",
        "applications",
        "intercom",
        "statuspage",
    ]
    assert conf.DASHBOARD_UI_SESSION_EXPIRATION_HOURS == 100
    assert conf.DASHBOARD_UI_DATADOG_CLIENT_TOKEN == "no-secret"
    assert conf.DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO == [1234, 5432]


def test_legacy_env_sets(
    original_environment_variables: None,
    unset_testing_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MERGIFYENGINE_BASE_URL", "https://dashboard.mergify.com")
    monkeypatch.setenv("MERGIFYENGINE_WEBHOOK_SECRET", "secret4")
    monkeypatch.setenv("MERGIFYENGINE_WEBHOOK_SECRET_PRE_ROTATION", "secret5")
    monkeypatch.setenv("MERGIFYENGINE_WEBHOOK_FORWARD_EVENT_TYPES", "foo,bar,yo")
    monkeypatch.setenv(
        "MERGIFYENGINE_WEBHOOK_APP_FORWARD_URL", "https://sub.example.com/events"
    )
    monkeypatch.setenv("MERGIFYENGINE_INTEGRATION_ID", "12345")
    monkeypatch.setenv("MERGIFYENGINE_PRIVATE_KEY", "aGVsbG8gd29ybGQ=")
    monkeypatch.setenv("MERGIFYENGINE_OAUTH_CLIENT_ID", "Iv1.XXXXXX")
    monkeypatch.setenv("MERGIFYENGINE_OAUTH_CLIENT_SECRET", "secret")
    conf = config.EngineSettings()
    assert conf.GITHUB_WEBHOOK_SECRET.get_secret_value() == "secret4"
    assert conf.GITHUB_WEBHOOK_SECRET_PRE_ROTATION is not None
    assert conf.GITHUB_WEBHOOK_SECRET_PRE_ROTATION.get_secret_value() == "secret5"
    assert conf.GITHUB_WEBHOOK_FORWARD_EVENT_TYPES == ["foo", "bar", "yo"]
    assert conf.GITHUB_WEBHOOK_FORWARD_URL == "https://sub.example.com/events"
    assert conf.GITHUB_APP_ID == 12345
    assert conf.GITHUB_PRIVATE_KEY.get_secret_value() == "hello world"
    assert conf.GITHUB_OAUTH_CLIENT_ID == "Iv1.XXXXXX"
    assert conf.GITHUB_OAUTH_CLIENT_SECRET.get_secret_value() == "secret"
    assert conf.DASHBOARD_UI_FRONT_URL == "https://dashboard.mergify.com"


@pytest.mark.parametrize(
    "path", ("/", "/foobar", "/foobar/", "?foobar=1", "/foobar/?foobar=1")
)
def test_github_url_normalization(
    original_environment_variables: None, monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_URL", f"https://my-ghes.example.com{path}")
    conf = config.EngineSettings()
    path, _, _ = path.partition("?")
    if path.endswith("/"):
        path = path[:-1]
    assert conf.GITHUB_URL == f"https://my-ghes.example.com{path}"
    assert conf.GITHUB_REST_API_URL == f"https://my-ghes.example.com{path}/api/v3"
    assert (
        conf.GITHUB_GRAPHQL_API_URL == f"https://my-ghes.example.com{path}/api/graphql"
    )


def test_database_url_replace(
    original_environment_variables: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "MERGIFYENGINE_DATABASE_URL", "postgres://user:password@example.com:1234/db"
    )
    conf = config.EngineSettings()
    assert str(conf.DATABASE_URL) == "postgresql+psycopg://***@example.com:1234/db"
    assert (
        conf.DATABASE_URL.geturl()
        == "postgresql+psycopg://user:password@example.com:1234/db"
    )

    # ensure we still protected after a _replace()
    new_url = conf.DATABASE_URL._replace(path="db2")
    assert isinstance(new_url, urls.PostgresDSN)
    assert str(new_url) == "postgresql+psycopg://***@example.com:1234/db2"
    assert new_url.geturl() == "postgresql+psycopg://user:password@example.com:1234/db2"


@pytest.mark.parametrize(
    "env,expected",
    (
        (
            # nosemgrep: generic.secrets.security.detected-username-and-password-in-uri.detected-username-and-password-in-uri
            "postgres://foo:bar@foobar.com:123/path",
            # nosemgrep: generic.secrets.security.detected-username-and-password-in-uri.detected-username-and-password-in-uri
            "postgresql+psycopg://foo:bar@foobar.com:123/path",
        ),
        (
            # nosemgrep: generic.secrets.security.detected-username-and-password-in-uri.detected-username-and-password-in-uri
            "postgresql://foo:bar@foobar.com:123/path",
            # nosemgrep: generic.secrets.security.detected-username-and-password-in-uri.detected-username-and-password-in-uri
            "postgresql+psycopg://foo:bar@foobar.com:123/path",
        ),
        (
            # nosemgrep: generic.secrets.security.detected-username-and-password-in-uri.detected-username-and-password-in-uri
            "postgres://foo:bar@foobar.com:123/path?azert=foo",
            # nosemgrep: generic.secrets.security.detected-username-and-password-in-uri.detected-username-and-password-in-uri
            "postgresql+psycopg://foo:bar@foobar.com:123/path?azert=foo",
        ),
        ("postgres://", "postgresql+psycopg:"),
        ("postgresql://", "postgresql+psycopg:"),
        ("postgresql+psycopg://", "postgresql+psycopg:"),
    ),
)
async def test_database_url_format(
    env: str, expected: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_URL", env)
    conf = config.EngineSettings()
    assert conf.DATABASE_URL.geturl() == expected


def test_error_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_URL", "https://localhost")
    with pytest.raises(pydantic.ValidationError) as exc_info:
        config.EngineSettings()

    assert (
        str(exc_info.value)
        == """1 validation error for EngineSettings
MERGIFYENGINE_DATABASE_URL
  scheme `https` is invalid, must be postgres,postgresql,postgresql+psycopg (type=value_error)"""
    )
