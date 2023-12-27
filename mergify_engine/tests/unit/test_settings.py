import logging
import os

import pydantic
import pydantic_core
import pytest

from mergify_engine import config
from mergify_engine.config import types


@pytest.fixture()
def _unset_testing_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(config.EngineSettings.model_config, "env_file", None)

    # NOTE(sileht): we can't use monkeypatch.delenv here because it doesn't
    # work well with _original_environment_variables() fixture
    # We don't need to restore the env variables because the _original_environment_variables() already does it.
    for env in list(os.environ.keys()):
        if env.startswith("MERGIFYENGINE"):
            del os.environ[env]


@pytest.fixture()
def _setup_mandatory_envs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_OAUTH_TOKEN_SECRET_CURRENT", "secret")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_WEBHOOK_SECRET", "secret")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_APP_ID", "12345")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_PRIVATE_KEY", "aGVsbG8gd29ybGQ=")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_OAUTH_CLIENT_ID", "Iv1.XXXXXX")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_OAUTH_CLIENT_SECRET", "secret")
    monkeypatch.setenv("MERGIFYENGINE_REDIS_CRYPTO_SECRET_CURRENT", "crypto-secret")


def test_defaults(
    _unset_testing_env: None,
    _setup_mandatory_envs: None,
) -> None:
    conf = config.EngineSettings()
    assert str(conf.DATABASE_URL) == "postgresql+psycopg://localhost:5432"
    assert conf.DATABASE_URL.geturl() == "postgresql+psycopg://localhost:5432"
    assert {"web": 55, "worker": 15} == conf.DATABASE_POOL_SIZES
    assert conf.DATABASE_OAUTH_TOKEN_SECRET_CURRENT.get_secret_value() == "secret"
    assert conf.DATABASE_OAUTH_TOKEN_SECRET_OLD is None
    assert conf.GITHUB_URL == "https://github.com"  # type: ignore[unreachable]
    assert conf.GITHUB_REST_API_URL == "https://api.github.com"
    assert conf.GITHUB_GRAPHQL_API_URL == "https://api.github.com/graphql"
    assert conf.GITHUB_APP_ID == 12345
    assert conf.GITHUB_PRIVATE_KEY.get_secret_value() == "hello world"
    assert conf.GITHUB_OAUTH_CLIENT_ID == "Iv1.XXXXXX"
    assert conf.GITHUB_OAUTH_CLIENT_SECRET.get_secret_value() == "secret"
    assert conf.GITHUB_WEBHOOK_SECRET.get_secret_value() == "secret"
    assert conf.GITHUB_WEBHOOK_SECRET_PRE_ROTATION is None
    assert [] == conf.GITHUB_WEBHOOK_FORWARD_EVENT_TYPES
    assert conf.GITHUB_WEBHOOK_FORWARD_URL is None
    assert conf.DASHBOARD_UI_STATIC_FILES_DIRECTORY is None
    assert conf.DASHBOARD_UI_FRONT_URL == "http://localhost:3000"
    assert [] == conf.DASHBOARD_UI_FEATURES
    assert conf.DASHBOARD_UI_SESSION_EXPIRATION_HOURS == 24
    assert conf.DASHBOARD_UI_DATADOG_CLIENT_TOKEN is None
    assert [] == conf.DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO
    assert conf.LOG_LEVEL == logging.INFO
    assert conf.LOG_STDOUT
    assert conf.LOG_STDOUT_LEVEL is None
    assert conf.LOG_DATADOG is False
    assert conf.LOG_DATADOG_LEVEL is None
    assert [] == conf.LOG_DEBUG_LOGGER_NAMES
    assert conf.SENTRY_URL is None
    assert conf.SENTRY_ENVIRONMENT == "test"

    assert conf.SHARED_STREAM_PROCESSES == 1
    assert conf.DEDICATED_STREAM_PROCESSES == 1
    assert conf.SHARED_STREAM_TASKS_PER_PROCESS == 7
    assert conf.BUCKET_PROCESSING_MAX_SECONDS == 30
    assert conf.MAX_GITTER_CONCURRENT_JOBS == 20

    assert conf.SUBSCRIPTION_TOKEN is None
    assert conf.ENGINE_TO_SHADOW_OFFICE_API_KEY.get_secret_value()
    assert conf.SUBSCRIPTION_URL == "https://subscription.mergify.com"
    assert conf.SHADOW_OFFICE_TO_ENGINE_API_KEY.get_secret_value()
    assert conf.SHADOW_OFFICE_TO_ENGINE_API_KEY_PRE_ROTATION is None
    assert [] == conf.ACCOUNT_TOKENS
    assert {} == conf.APPLICATION_APIKEYS

    assert conf.REDIS_CRYPTO_SECRET_CURRENT.get_secret_value() == "crypto-secret"
    assert conf.REDIS_CRYPTO_SECRET_OLD is None

    assert conf.DEFAULT_REDIS_URL.geturl() == "redis://localhost:6379"
    assert conf.REDIS_STREAM_WEB_MAX_CONNECTIONS == 50
    assert conf.REDIS_CACHE_WEB_MAX_CONNECTIONS == 50
    assert conf.REDIS_QUEUE_WEB_MAX_CONNECTIONS == 50
    assert conf.REDIS_STATS_WEB_MAX_CONNECTIONS == 50
    assert conf.REDIS_ACTIVE_USERS_WEB_MAX_CONNECTIONS == 50
    assert conf.REDIS_AUTHENTICATION_WEB_MAX_CONNECTIONS == 50
    assert conf.REDIS_SSL_VERIFY_MODE_CERT_NONE is False

    assert conf.VERSION == "dev"
    assert conf.SHA == "unknown"


def test_all_sets(
    _unset_testing_env: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    monkeypatch.setitem(config.EngineSettings.model_config, "env_file", None)

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
        "MERGIFYENGINE_GITHUB_WEBHOOK_FORWARD_URL",
        "https://sub.example.com/events",
    )
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_POOL_SIZES", "web:2,worker:3,foobar:6")
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_OAUTH_TOKEN_SECRET_CURRENT", "secret2")
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_OAUTH_TOKEN_SECRET_OLD", "secret3")
    monkeypatch.setenv(
        "MERGIFYENGINE_DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO",
        "1234,5432",
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
    monkeypatch.setenv("MERGIFYENGINE_LOG_DATADOG", "udp://localhost:8080")
    monkeypatch.setenv("MERGIFYENGINE_LOG_DATADOG_LEVEL", "WARNING")
    monkeypatch.setenv("MERGIFYENGINE_LOG_DEBUG_LOGGER_NAMES", "foo,bar,yo")

    monkeypatch.setenv("MERGIFYENGINE_SHARED_STREAM_PROCESSES", "2")
    monkeypatch.setenv("MERGIFYENGINE_DEDICATED_STREAM_PROCESSES", "3")
    monkeypatch.setenv("MERGIFYENGINE_SHARED_STREAM_TASKS_PER_PROCESS", "14")
    monkeypatch.setenv("MERGIFYENGINE_BUCKET_PROCESSING_MAX_SECONDS", "60")
    monkeypatch.setenv("MERGIFYENGINE_MAX_GITTER_CONCURRENT_JOBS", "40")

    monkeypatch.setenv(
        "MERGIFYENGINE_ENGINE_TO_SHADOW_OFFICE_API_KEY",
        "api-secret-key",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_SHADOW_OFFICE_TO_ENGINE_API_KEY",
        "webhook-secret",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_SHADOW_OFFICE_TO_ENGINE_API_KEY_PRE_ROTATION",
        "webhook-secret-bis",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_SUBSCRIPTION_URL",
        "https://subscription.example.com",
    )
    monkeypatch.setenv("MERGIFYENGINE_SUBSCRIPTION_TOKEN", "onprem-token")
    monkeypatch.setenv(
        "MERGIFYENGINE_APPLICATION_APIKEYS",
        ",".join(
            (
                "peeph4iephaivohx4jeewociex3ruliiShai1Auyiekekeij4OeGh0OoGuph5zei:12345:login",
                "tha0naCiWooj1yieV3AeChuDiaY9ieweiquahch3rib3quae3eP7sae7gohQuohB:54321:bot",
                "Ub5kiekohyuoqua5oori7Moowuom8yiefiequie3yohmo6Eidieb9eihiepi5aiP:424242:other-bot",
            ),
        ),
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_ACCOUNT_TOKENS",
        ",".join(
            (
                "123:login1:token1",
                "456:login2:token2",
            ),
        ),
    )
    monkeypatch.setenv("MERGIFYENGINE_REDIS_CRYPTO_SECRET_CURRENT", "crypto-secret")
    monkeypatch.setenv("MERGIFYENGINE_REDIS_CRYPTO_SECRET_OLD", "crypto-secret-old")
    monkeypatch.setenv("MERGIFYENGINE_LOG_EMBEDDER_ENABLED_ORGS", "Mergifyio")

    monkeypatch.setenv("MERGIFYENGINE_REDIS_STREAM_WEB_MAX_CONNECTIONS", "51")
    monkeypatch.setenv("MERGIFYENGINE_REDIS_CACHE_WEB_MAX_CONNECTIONS", "52")
    monkeypatch.setenv("MERGIFYENGINE_REDIS_QUEUE_WEB_MAX_CONNECTIONS", "53")
    monkeypatch.setenv("MERGIFYENGINE_REDIS_STATS_WEB_MAX_CONNECTIONS", "55")
    monkeypatch.setenv("MERGIFYENGINE_REDIS_ACTIVE_USERS_WEB_MAX_CONNECTIONS", "56")
    monkeypatch.setenv("MERGIFYENGINE_REDIS_AUTHENTICATION_WEB_MAX_CONNECTIONS", "57")
    monkeypatch.setenv("MERGIFYENGINE_REDIS_SSL_VERIFY_MODE_CERT_NONE", "True")
    monkeypatch.setenv("MERGIFYENGINE_DEFAULT_REDIS_URL", "redis://example.com:1234")
    monkeypatch.setenv("MERGIFYENGINE_SHA", "f8c4fe06d56cd89cbe48975aa8507d479d881bdc")
    monkeypatch.setenv("MERGIFYENGINE_VERSION", "3.1")

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
    assert ["foo", "bar", "yo"] == conf.GITHUB_WEBHOOK_FORWARD_EVENT_TYPES
    assert conf.GITHUB_WEBHOOK_FORWARD_URL == "https://sub.example.com/events"
    assert {"web": 2, "worker": 3, "foobar": 6} == conf.DATABASE_POOL_SIZES
    assert conf.DATABASE_OAUTH_TOKEN_SECRET_CURRENT.get_secret_value() == "secret2"
    assert conf.DATABASE_OAUTH_TOKEN_SECRET_OLD is not None
    assert conf.DATABASE_OAUTH_TOKEN_SECRET_OLD.get_secret_value() == "secret3"
    assert tmpdir == conf.DASHBOARD_UI_STATIC_FILES_DIRECTORY
    assert conf.DASHBOARD_UI_FRONT_URL == "https://dashboard.mergify.com"
    assert [
        "subscriptions",
        "applications",
        "intercom",
        "statuspage",
    ] == conf.DASHBOARD_UI_FEATURES
    assert conf.DASHBOARD_UI_SESSION_EXPIRATION_HOURS == 100
    assert conf.DASHBOARD_UI_DATADOG_CLIENT_TOKEN == "no-secret"
    assert [1234, 5432] == conf.DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO
    assert conf.LOG_LEVEL == logging.DEBUG
    assert conf.LOG_STDOUT is False
    assert conf.LOG_STDOUT_LEVEL == logging.CRITICAL
    assert isinstance(conf.LOG_DATADOG, pydantic_core.Url)
    assert str(conf.LOG_DATADOG) == "udp://localhost:8080"
    assert conf.LOG_DATADOG.scheme == "udp"
    assert conf.LOG_DATADOG.host == "localhost"
    assert conf.LOG_DATADOG.port == 8080
    assert conf.LOG_DATADOG_LEVEL == logging.WARNING
    assert ["foo", "bar", "yo"] == conf.LOG_DEBUG_LOGGER_NAMES

    assert conf.SHARED_STREAM_PROCESSES == 2
    assert conf.DEDICATED_STREAM_PROCESSES == 3
    assert conf.SHARED_STREAM_TASKS_PER_PROCESS == 14
    assert conf.BUCKET_PROCESSING_MAX_SECONDS == 60
    assert conf.MAX_GITTER_CONCURRENT_JOBS == 40

    assert conf.SUBSCRIPTION_TOKEN is not None
    assert conf.SUBSCRIPTION_TOKEN.get_secret_value() == "onprem-token"
    assert conf.ENGINE_TO_SHADOW_OFFICE_API_KEY.get_secret_value() == "api-secret-key"
    assert conf.SUBSCRIPTION_URL == "https://subscription.example.com"
    assert conf.SHADOW_OFFICE_TO_ENGINE_API_KEY.get_secret_value() == "webhook-secret"
    assert conf.SHADOW_OFFICE_TO_ENGINE_API_KEY_PRE_ROTATION is not None
    assert (
        conf.SHADOW_OFFICE_TO_ENGINE_API_KEY_PRE_ROTATION.get_secret_value()
        == "webhook-secret-bis"
    )
    assert [
        (123, "login1", pydantic.SecretStr("token1")),
        (456, "login2", pydantic.SecretStr("token2")),
    ] == conf.ACCOUNT_TOKENS
    assert {
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
    } == conf.APPLICATION_APIKEYS

    assert conf.REDIS_CRYPTO_SECRET_CURRENT.get_secret_value() == "crypto-secret"
    assert conf.REDIS_CRYPTO_SECRET_OLD is not None
    assert conf.REDIS_CRYPTO_SECRET_OLD.get_secret_value() == "crypto-secret-old"

    assert conf.DEFAULT_REDIS_URL.geturl() == "redis://example.com:1234"
    assert conf.REDIS_STREAM_WEB_MAX_CONNECTIONS == 51
    assert conf.REDIS_CACHE_WEB_MAX_CONNECTIONS == 52
    assert conf.REDIS_QUEUE_WEB_MAX_CONNECTIONS == 53
    assert conf.REDIS_STATS_WEB_MAX_CONNECTIONS == 55
    assert conf.REDIS_ACTIVE_USERS_WEB_MAX_CONNECTIONS == 56
    assert conf.REDIS_AUTHENTICATION_WEB_MAX_CONNECTIONS == 57
    assert conf.REDIS_SSL_VERIFY_MODE_CERT_NONE is True
    assert conf.VERSION == "3.1"
    assert conf.SHA == "f8c4fe06d56cd89cbe48975aa8507d479d881bdc"
    assert ["Mergifyio"] == conf.LOG_EMBEDDER_ENABLED_ORGS


def test_legacy_env_sets(
    _unset_testing_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_OAUTH_TOKEN_SECRET_CURRENT", "secret")
    monkeypatch.setenv("MERGIFYENGINE_BASE_URL", "https://dashboard.mergify.com")
    monkeypatch.setenv("MERGIFYENGINE_WEBHOOK_SECRET", "secret4")
    monkeypatch.setenv("MERGIFYENGINE_WEBHOOK_SECRET_PRE_ROTATION", "secret5")
    monkeypatch.setenv("MERGIFYENGINE_WEBHOOK_FORWARD_EVENT_TYPES", "foo,bar,yo")
    monkeypatch.setenv(
        "MERGIFYENGINE_WEBHOOK_APP_FORWARD_URL",
        "https://sub.example.com/events",
    )
    monkeypatch.setenv("MERGIFYENGINE_INTEGRATION_ID", "12345")
    monkeypatch.setenv("MERGIFYENGINE_PRIVATE_KEY", "aGVsbG8gd29ybGQ=")
    monkeypatch.setenv("MERGIFYENGINE_OAUTH_CLIENT_ID", "Iv1.XXXXXX")
    monkeypatch.setenv("MERGIFYENGINE_OAUTH_CLIENT_SECRET", "secret")

    monkeypatch.setenv(
        "MERGIFYENGINE_SUBSCRIPTION_BASE_URL",
        "https://subscription.example.com",
    )
    monkeypatch.setenv("MERGIFYENGINE_SUBSCRIPTION_TOKEN", "onprem-token")
    monkeypatch.setenv("MERGIFYENGINE_CACHE_TOKEN_SECRET", "crypto-secret")
    monkeypatch.setenv("MERGIFYENGINE_CACHE_TOKEN_SECRET_OLD", "crypto-secret-old")

    monkeypatch.setenv("MERGIFYENGINE_ENGINE_TO_DASHBOARD_API_KEY", "api-secret-key")
    monkeypatch.setenv("MERGIFYENGINE_DASHBOARD_TO_ENGINE_API_KEY", "webhook-secret")
    monkeypatch.setenv(
        "MERGIFYENGINE_DASHBOARD_TO_ENGINE_API_KEY_PRE_ROTATION",
        "webhook-secret-bis",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_LOG_EMBEDDER_METADATA_EXTRACT_MODEL",
        "gpt-3.5-turbo-1106",
    )

    conf = config.EngineSettings()
    assert conf.GITHUB_WEBHOOK_SECRET.get_secret_value() == "secret4"
    assert conf.GITHUB_WEBHOOK_SECRET_PRE_ROTATION is not None
    assert conf.GITHUB_WEBHOOK_SECRET_PRE_ROTATION.get_secret_value() == "secret5"
    assert ["foo", "bar", "yo"] == conf.GITHUB_WEBHOOK_FORWARD_EVENT_TYPES
    assert conf.GITHUB_WEBHOOK_FORWARD_URL == "https://sub.example.com/events"
    assert conf.GITHUB_APP_ID == 12345
    assert conf.GITHUB_PRIVATE_KEY.get_secret_value() == "hello world"
    assert conf.GITHUB_OAUTH_CLIENT_ID == "Iv1.XXXXXX"
    assert conf.GITHUB_OAUTH_CLIENT_SECRET.get_secret_value() == "secret"
    assert conf.DASHBOARD_UI_FRONT_URL == "https://dashboard.mergify.com"
    assert conf.SUBSCRIPTION_URL == "https://subscription.example.com"

    assert conf.REDIS_CRYPTO_SECRET_CURRENT.get_secret_value() == "crypto-secret"
    assert conf.REDIS_CRYPTO_SECRET_OLD is not None
    assert conf.REDIS_CRYPTO_SECRET_OLD.get_secret_value() == "crypto-secret-old"

    assert conf.ENGINE_TO_SHADOW_OFFICE_API_KEY.get_secret_value() == "api-secret-key"
    assert conf.SHADOW_OFFICE_TO_ENGINE_API_KEY.get_secret_value() == "webhook-secret"
    assert conf.SHADOW_OFFICE_TO_ENGINE_API_KEY_PRE_ROTATION is not None
    assert (
        conf.SHADOW_OFFICE_TO_ENGINE_API_KEY_PRE_ROTATION.get_secret_value()
        == "webhook-secret-bis"
    )
    assert conf.LOG_EMBEDDER_METADATA_EXTRACT_MODEL == "gpt-3.5-turbo-1106"


@pytest.mark.parametrize(
    "env_var",
    ("MERGIFYENGINE_BASE_URL", "MERGIFYENGINE_DASHBOARD_UI_FRONT_BASE_URL"),
)
def test_legacy_dashboard_urls(
    _unset_testing_env: None,
    _setup_mandatory_envs: None,
    monkeypatch: pytest.MonkeyPatch,
    env_var: str,
) -> None:
    monkeypatch.setenv("MERGIFYENGINE_BASE_URL", "https://not-me-for-sure.example.com")

    monkeypatch.setenv(env_var, "https://mergify.example.com")
    conf = config.EngineSettings()
    assert conf.DASHBOARD_UI_FRONT_URL == "https://mergify.example.com"


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        ("10", 10),
        ("1000", 1000),
        (
            "-100",
            """1 validation error for EngineSettings
LOG_LEVEL
  Input should be greater than 0 [type=greater_than, input_value=-100, input_type=int]""",
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
            """1 validation error for EngineSettings
LOG_LEVEL
  Value error, invalid literal for int() with base 10: 'NOT_A_STRING_LEVEL' [type=value_error, input_value='NOT_A_STRING_LEVEL', input_type=str]""",
        ),
    ),
)
def test_type_log_level(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    expected: int | str,
) -> None:
    monkeypatch.setenv("MERGIFYENGINE_LOG_LEVEL", value)
    if isinstance(expected, int):
        conf = config.EngineSettings()
        assert expected == conf.LOG_LEVEL
    else:
        with pytest.raises(pydantic_core.ValidationError) as exc_info:
            config.EngineSettings()

        assert str(exc_info.value).startswith(expected)


@pytest.mark.parametrize(
    "path",
    ("/", "/foobar", "/foobar/", "?foobar=1", "/foobar/?foobar=1"),
)
def test_github_url_normalization(monkeypatch: pytest.MonkeyPatch, path: str) -> None:
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_URL", f"https://my-ghes.example.com{path}")
    conf = config.EngineSettings()
    path, _, _ = path.partition("?")
    if path.endswith("/"):
        path = path[:-1]
    assert f"https://my-ghes.example.com{path}" == conf.GITHUB_URL
    assert f"https://my-ghes.example.com{path}/api/v3" == conf.GITHUB_REST_API_URL
    assert (
        f"https://my-ghes.example.com{path}/api/graphql" == conf.GITHUB_GRAPHQL_API_URL
    )


def test_database_url_replace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "MERGIFYENGINE_DATABASE_URL",
        "postgres://user:password@example.com:1234/db",
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
    ("env", "expected"),
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
    env: str,
    expected: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_URL", env)
    conf = config.EngineSettings()
    assert conf.DATABASE_URL.geturl() == expected


def test_error_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_URL", "https://localhost")
    with pytest.raises(pydantic_core.ValidationError) as exc_info:
        config.EngineSettings()

    assert str(exc_info.value).startswith(
        """1 validation error for EngineSettings
DATABASE_URL
  Value error, scheme `https` is invalid, must be postgres,postgresql,postgresql+psycopg [type=value_error, input_value='https://localhost', input_type=str]""",
    )


def test_redis_onpremise_legacy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MERGIFYENGINE_DEFAULT_REDIS_URL")
    monkeypatch.setenv("MERGIFYENGINE_STORAGE_URL", "rediss://redis.example.com:1234")
    conf = config.EngineSettings()
    assert conf.STREAM_URL.geturl() == "rediss://redis.example.com:1234"
    assert conf.QUEUE_URL.geturl() == "rediss://redis.example.com:1234"
    assert conf.CACHE_URL.geturl() == "rediss://redis.example.com:1234"
    assert (
        conf.TEAM_MEMBERS_CACHE_URL.geturl() == "rediss://redis.example.com:1234?db=5"
    )
    assert (
        conf.TEAM_PERMISSIONS_CACHE_URL.geturl()
        == "rediss://redis.example.com:1234?db=6"
    )
    assert (
        conf.USER_PERMISSIONS_CACHE_URL.geturl()
        == "rediss://redis.example.com:1234?db=7"
    )


def test_redis_saas_current(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "MERGIFYENGINE_DEFAULT_REDIS_URL",
        "rediss://redis.example.com:1234",
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
        "MERGIFYENGINE_QUEUE_URL",
        "rediss://redis-queue.example.com:1234?db=4",
    )
    conf = config.EngineSettings()
    assert conf.DEFAULT_REDIS_URL.geturl() == "rediss://redis.example.com:1234"
    assert (
        conf.CACHE_URL.geturl() == "rediss://redis-legacy-cache.example.com:1234?db=2"
    )
    assert conf.STREAM_URL.geturl() == "rediss://redis-stream.example.com:1234?db=3"
    assert conf.QUEUE_URL.geturl() == "rediss://redis-queue.example.com:1234?db=4"
    assert (
        conf.TEAM_MEMBERS_CACHE_URL.geturl() == "rediss://redis.example.com:1234?db=5"
    )
    assert (
        conf.TEAM_PERMISSIONS_CACHE_URL.geturl()
        == "rediss://redis.example.com:1234?db=6"
    )
    assert (
        conf.USER_PERMISSIONS_CACHE_URL.geturl()
        == "rediss://redis.example.com:1234?db=7"
    )


def test_redis_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "MERGIFYENGINE_DEFAULT_REDIS_URL",
        "rediss://redis.example.com:1234",
    )
    conf = config.EngineSettings()
    assert conf.CACHE_URL.geturl() == "rediss://redis.example.com:1234?db=2"
    assert conf.STREAM_URL.geturl() == "rediss://redis.example.com:1234?db=3"
    assert conf.QUEUE_URL.geturl() == "rediss://redis.example.com:1234?db=4"
    assert (
        conf.TEAM_MEMBERS_CACHE_URL.geturl() == "rediss://redis.example.com:1234?db=5"
    )
    assert (
        conf.TEAM_PERMISSIONS_CACHE_URL.geturl()
        == "rediss://redis.example.com:1234?db=6"
    )
    assert (
        conf.USER_PERMISSIONS_CACHE_URL.geturl()
        == "rediss://redis.example.com:1234?db=7"
    )


def test_redis_all_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "MERGIFYENGINE_DEFAULT_REDIS_URL",
        "rediss://redis-default.example.com:1234",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_CACHE_URL",
        "rediss://redis-legacy-cache.example.com:1234",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_STREAM_URL",
        "rediss://redis-stream.example.com:1234",
    )
    monkeypatch.setenv(
        "MERGIFYENGINE_QUEUE_URL",
        "rediss://redis-queue.example.com:1234",
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
    conf = config.EngineSettings()
    assert conf.CACHE_URL.geturl() == "rediss://redis-legacy-cache.example.com:1234"
    assert conf.STREAM_URL.geturl() == "rediss://redis-stream.example.com:1234"
    assert conf.QUEUE_URL.geturl() == "rediss://redis-queue.example.com:1234"
    assert (
        conf.TEAM_MEMBERS_CACHE_URL.geturl()
        == "rediss://redis-team-members.example.com:1234"
    )
    assert (
        conf.TEAM_PERMISSIONS_CACHE_URL.geturl()
        == "rediss://redis-team-perm.example.com:1234"
    )
    assert (
        conf.USER_PERMISSIONS_CACHE_URL.geturl()
        == "rediss://redis-user-perm.example.com:1234"
    )


def test_invalid_openai_model(
    _unset_testing_env: None,
    _setup_mandatory_envs: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "MERGIFYENGINE_LOG_EMBEDDER_METADATA_EXTRACT_MODEL",
        "gpt-3.5-invalid",
    )
    with pytest.raises(pydantic_core.ValidationError) as exc_info:
        config.EngineSettings()

    assert "Input should be 'gpt-4-1106-preview' or 'gpt-3.5-turbo-1106'" in str(
        exc_info.value,
    )
