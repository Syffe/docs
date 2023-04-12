import logging
import os

import pydantic
import pytest

from mergify_engine import config
from mergify_engine.config import types


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
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_OAUTH_TOKEN_SECRET_CURRENT", "secret")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_WEBHOOK_SECRET", "secret")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_APP_ID", "12345")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_PRIVATE_KEY", "aGVsbG8gd29ybGQ=")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_OAUTH_CLIENT_ID", "Iv1.XXXXXX")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_OAUTH_CLIENT_SECRET", "secret")

    conf = config.EngineSettings()
    assert str(conf.DATABASE_URL) == "postgresql+psycopg://localhost:5432"
    assert conf.DATABASE_URL.geturl() == "postgresql+psycopg://localhost:5432"
    assert conf.DATABASE_POOL_SIZES == {"web": 55, "worker": 15}
    assert conf.DATABASE_OAUTH_TOKEN_SECRET_CURRENT.get_secret_value() == "secret"
    assert conf.DATABASE_OAUTH_TOKEN_SECRET_OLD is None
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
    assert conf.DASHBOARD_UI_FRONT_URL == "http://localhost:3000"
    assert conf.DASHBOARD_UI_FEATURES == []
    assert conf.DASHBOARD_UI_SESSION_EXPIRATION_HOURS == 24
    assert conf.DASHBOARD_UI_DATADOG_CLIENT_TOKEN is None
    assert conf.DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO == []
    assert conf.LOG_LEVEL == logging.INFO
    assert conf.LOG_STDOUT
    assert conf.LOG_STDOUT_LEVEL is None
    assert conf.LOG_DATADOG is False
    assert conf.LOG_DATADOG_LEVEL is None
    assert conf.LOG_DEBUG_LOGGER_NAMES == []
    assert conf.SENTRY_URL is None
    assert conf.SENTRY_ENVIRONMENT == "test"

    assert conf.SHARED_STREAM_PROCESSES == 1
    assert conf.DEDICATED_STREAM_PROCESSES == 1
    assert conf.SHARED_STREAM_TASKS_PER_PROCESS == 7
    assert conf.BUCKET_PROCESSING_MAX_SECONDS == 30
    assert conf.MAX_GITTER_CONCURRENT_JOBS == 20

    assert conf.SUBSCRIPTION_TOKEN is None
    assert conf.ENGINE_TO_DASHBOARD_API_KEY.get_secret_value()
    assert conf.SUBSCRIPTION_URL == "https://subscription.mergify.com"
    assert conf.DASHBOARD_TO_ENGINE_API_KEY.get_secret_value()
    assert conf.DASHBOARD_TO_ENGINE_API_KEY_PRE_ROTATION is None
    assert conf.ACCOUNT_TOKENS == []
    assert conf.APPLICATION_APIKEYS == {}


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
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_OAUTH_TOKEN_SECRET_CURRENT", "secret2")
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_OAUTH_TOKEN_SECRET_OLD", "secret3")
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
    monkeypatch.setenv("MERGIFYENGINE_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("MERGIFYENGINE_LOG_STDOUT", "false")
    monkeypatch.setenv("MERGIFYENGINE_LOG_STDOUT_LEVEL", "CRITICAL")
    monkeypatch.setenv("MERGIFYENGINE_LOG_DATADOG", "true")
    monkeypatch.setenv("MERGIFYENGINE_LOG_DATADOG_LEVEL", "WARNING")
    monkeypatch.setenv("MERGIFYENGINE_LOG_DEBUG_LOGGER_NAMES", "foo,bar,yo")

    monkeypatch.setenv("MERGIFYENGINE_SHARED_STREAM_PROCESSES", "2")
    monkeypatch.setenv("MERGIFYENGINE_DEDICATED_STREAM_PROCESSES", "3")
    monkeypatch.setenv("MERGIFYENGINE_SHARED_STREAM_TASKS_PER_PROCESS", "14")
    monkeypatch.setenv("MERGIFYENGINE_BUCKET_PROCESSING_MAX_SECONDS", "60")
    monkeypatch.setenv("MERGIFYENGINE_MAX_GITTER_CONCURRENT_JOBS", "40")

    monkeypatch.setenv("MERGIFYENGINE_ENGINE_TO_DASHBOARD_API_KEY", "api-secret-key")
    monkeypatch.setenv("MERGIFYENGINE_DASHBOARD_TO_ENGINE_API_KEY", "webhook-secret")
    monkeypatch.setenv(
        "MERGIFYENGINE_DASHBOARD_TO_ENGINE_API_KEY_PRE_ROTATION", "webhook-secret-bis"
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_SUBSCRIPTION_URL", "https://subscription.example.com"
    )
    monkeypatch.setenv("MERGIFYENGINE_SUBSCRIPTION_TOKEN", "onprem-token")
    monkeypatch.setenv(
        "MERGIFYENGINE_APPLICATION_APIKEYS",
        ",".join(
            (
                "peeph4iephaivohx4jeewociex3ruliiShai1Auyiekekeij4OeGh0OoGuph5zei:12345:login",
                "tha0naCiWooj1yieV3AeChuDiaY9ieweiquahch3rib3quae3eP7sae7gohQuohB:54321:bot",
                "Ub5kiekohyuoqua5oori7Moowuom8yiefiequie3yohmo6Eidieb9eihiepi5aiP:424242:other-bot",
            )
        ),
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
    assert conf.DATABASE_OAUTH_TOKEN_SECRET_CURRENT.get_secret_value() == "secret2"
    assert conf.DATABASE_OAUTH_TOKEN_SECRET_OLD is not None
    assert conf.DATABASE_OAUTH_TOKEN_SECRET_OLD.get_secret_value() == "secret3"
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
    assert conf.LOG_LEVEL == logging.DEBUG
    assert conf.LOG_STDOUT is False
    assert conf.LOG_STDOUT_LEVEL == logging.CRITICAL
    assert conf.LOG_DATADOG is True
    assert conf.LOG_DATADOG_LEVEL == logging.WARNING
    assert conf.LOG_DEBUG_LOGGER_NAMES == ["foo", "bar", "yo"]

    assert conf.SHARED_STREAM_PROCESSES == 2
    assert conf.DEDICATED_STREAM_PROCESSES == 3
    assert conf.SHARED_STREAM_TASKS_PER_PROCESS == 14
    assert conf.BUCKET_PROCESSING_MAX_SECONDS == 60
    assert conf.MAX_GITTER_CONCURRENT_JOBS == 40

    assert conf.SUBSCRIPTION_TOKEN is not None
    assert conf.SUBSCRIPTION_TOKEN.get_secret_value() == "onprem-token"
    assert conf.ENGINE_TO_DASHBOARD_API_KEY.get_secret_value() == "api-secret-key"
    assert conf.SUBSCRIPTION_URL == "https://subscription.example.com"
    assert conf.DASHBOARD_TO_ENGINE_API_KEY.get_secret_value() == "webhook-secret"
    assert conf.DASHBOARD_TO_ENGINE_API_KEY_PRE_ROTATION is not None
    assert (
        conf.DASHBOARD_TO_ENGINE_API_KEY_PRE_ROTATION.get_secret_value()
        == "webhook-secret-bis"
    )
    assert conf.ACCOUNT_TOKENS == []
    assert conf.APPLICATION_APIKEYS == {
        "peeph4iephaivohx4jeewociex3rulii": {
            "api_access_key": "peeph4iephaivohx4jeewociex3rulii",
            "api_secret_key": "Shai1Auyiekekeij4OeGh0OoGuph5zei",
            "account_id": 12345,
            "account_login": "login",
        },
        "tha0naCiWooj1yieV3AeChuDiaY9iewe": {
            "api_access_key": "tha0naCiWooj1yieV3AeChuDiaY9iewe",
            "api_secret_key": "iquahch3rib3quae3eP7sae7gohQuohB",
            "account_id": 54321,
            "account_login": "bot",
        },
        "Ub5kiekohyuoqua5oori7Moowuom8yie": {
            "api_access_key": "Ub5kiekohyuoqua5oori7Moowuom8yie",
            "api_secret_key": "fiequie3yohmo6Eidieb9eihiepi5aiP",
            "account_id": 424242,
            "account_login": "other-bot",
        },
    }


def test_legacy_env_sets(
    original_environment_variables: None,
    unset_testing_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_OAUTH_TOKEN_SECRET_CURRENT", "secret")
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

    monkeypatch.setenv(
        "MERGIFYENGINE_SUBSCRIPTION_BASE_URL", "https://subscription.example.com"
    )
    monkeypatch.setenv("MERGIFYENGINE_SUBSCRIPTION_TOKEN", "onprem-token")

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
    assert conf.SUBSCRIPTION_URL == "https://subscription.example.com"


@pytest.mark.parametrize(
    "env_var", ("MERGIFYENGINE_BASE_URL", "MERGIFYENGINE_DASHBOARD_UI_FRONT_BASE_URL")
)
def test_legacy_dashboard_urls(
    original_environment_variables: None,
    unset_testing_env: None,
    monkeypatch: pytest.MonkeyPatch,
    env_var: str,
) -> None:
    # Required values
    monkeypatch.setenv("MERGIFYENGINE_BASE_URL", "https://not-me-for-sure.example.com")
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_OAUTH_TOKEN_SECRET_CURRENT", "secret")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_WEBHOOK_SECRET", "secret")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_APP_ID", "12345")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_PRIVATE_KEY", "aGVsbG8gd29ybGQ=")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_OAUTH_CLIENT_ID", "Iv1.XXXXXX")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_OAUTH_CLIENT_SECRET", "secret")

    monkeypatch.setenv(env_var, "https://mergify.example.com")
    conf = config.EngineSettings()
    assert conf.DASHBOARD_UI_FRONT_URL == "https://mergify.example.com"


@pytest.mark.parametrize(
    "value, expected",
    (
        ("10", 10),
        ("1000", 1000),
        (
            "-100",
            """
1 validation error for EngineSettings
MERGIFYENGINE_LOG_LEVEL
  ensure this value is greater than 0 (type=value_error.number.not_gt; limit_value=0)
""".strip(),
        ),
        ("INFO", logging.INFO),
        ("ERROR", logging.ERROR),
        ("WARNING", logging.WARNING),
        ("DEBUG", logging.DEBUG),
        ("CRITICAL", logging.CRITICAL),
        ("info", logging.INFO),
        ("debug", logging.DEBUG),
        (
            "NOT_A_STRING_LEVEL",
            """
1 validation error for EngineSettings
MERGIFYENGINE_LOG_LEVEL
  value is not a valid integer (type=type_error.integer)
""".strip(),
        ),
    ),
)
def test_type_log_level(
    original_environment_variables: None,
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    expected: int | None,
) -> None:
    monkeypatch.setenv("MERGIFYENGINE_LOG_LEVEL", value)
    if isinstance(expected, int):
        conf = config.EngineSettings()
        assert conf.LOG_LEVEL == expected
    else:
        with pytest.raises(pydantic.ValidationError) as exc_info:
            config.EngineSettings()

        assert str(exc_info.value) == expected


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
    assert isinstance(new_url, types.PostgresDSN)
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
