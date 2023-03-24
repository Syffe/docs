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
    conf = config.EngineSettings()
    assert str(conf.DATABASE_URL) == "postgresql+psycopg://localhost:5432"
    assert conf.DATABASE_URL.geturl() == "postgresql+psycopg://localhost:5432"
    assert conf.DATABASE_POOL_SIZES == {"web": 55, "worker": 15}
    assert conf.GITHUB_WEBHOOK_SECRET.get_secret_value() == "secret"
    assert conf.GITHUB_WEBHOOK_SECRET_PRE_ROTATION is None
    assert conf.GITHUB_WEBHOOK_FORWARD_EVENT_TYPES == []


def test_all_sets(
    original_environment_variables: None,
    unset_testing_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config.EngineSettings.Config, "env_file", None)

    monkeypatch.setenv("MERGIFYENGINE_GITHUB_WEBHOOK_SECRET", "secret2")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_WEBHOOK_FORWARD_EVENT_TYPES", "foo,bar,yo")
    monkeypatch.setenv("MERGIFYENGINE_GITHUB_WEBHOOK_SECRET_PRE_ROTATION", "secret3")
    monkeypatch.setenv("MERGIFYENGINE_DATABASE_POOL_SIZES", "web:2,worker:3,foobar:6")

    conf = config.EngineSettings()
    assert conf.GITHUB_WEBHOOK_SECRET.get_secret_value() == "secret2"
    assert conf.GITHUB_WEBHOOK_SECRET_PRE_ROTATION is not None
    assert conf.GITHUB_WEBHOOK_SECRET_PRE_ROTATION.get_secret_value() == "secret3"
    assert conf.GITHUB_WEBHOOK_FORWARD_EVENT_TYPES == ["foo", "bar", "yo"]
    assert conf.DATABASE_POOL_SIZES == {"web": 2, "worker": 3, "foobar": 6}


def test_legacy_env_sets(
    original_environment_variables: None,
    unset_testing_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MERGIFYENGINE_WEBHOOK_SECRET", "secret4")
    monkeypatch.setenv("MERGIFYENGINE_WEBHOOK_SECRET_PRE_ROTATION", "secret5")
    monkeypatch.setenv("MERGIFYENGINE_WEBHOOK_FORWARD_EVENT_TYPES", "foo,bar,yo")
    conf = config.EngineSettings()
    assert conf.GITHUB_WEBHOOK_SECRET.get_secret_value() == "secret4"
    assert conf.GITHUB_WEBHOOK_SECRET_PRE_ROTATION is not None
    assert conf.GITHUB_WEBHOOK_SECRET_PRE_ROTATION.get_secret_value() == "secret5"
    assert conf.GITHUB_WEBHOOK_FORWARD_EVENT_TYPES == ["foo", "bar", "yo"]


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
