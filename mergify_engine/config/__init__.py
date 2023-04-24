import base64
import collections
import os
import secrets
import sys
import typing
from urllib import parse

import pydantic

from mergify_engine import github_types
from mergify_engine import utils
from mergify_engine.config import types


CONFIGURATION_FILE = os.getenv("MERGIFYENGINE_TEST_SETTINGS")

DASHBOARD_DEFAULT_URL = pydantic.HttpUrl("http://localhost:3000", scheme="http")


class SecretStrFromBase64(pydantic.SecretStr):
    def __init__(self, value: str):
        super().__init__(base64.b64decode(value).decode())


class DatabaseSettings(pydantic.BaseSettings):
    DATABASE_URL: types.PostgresDSN = types.PostgresDSN.parse(
        "postgres://localhost:5432"
    )
    DATABASE_POOL_SIZES: dict[str, int] = pydantic.Field(
        default={"worker": 15, "web": 55}
    )
    DATABASE_OAUTH_TOKEN_SECRET_CURRENT: pydantic.SecretStr
    DATABASE_OAUTH_TOKEN_SECRET_OLD: pydantic.SecretStr | None = None


# NOTE(sileht): If we reach 15, we should update onpremise installation guide
# and add an upgrade release note section to ensure people configure their Redis
# correctly
REDIS_AUTO_DB_SHARDING_MAPPING = {
    # 0 reserved, never use it, this force people to select a DB before running any command
    # and maybe be used by legacy onpremise installation.
    # 1 temporary reserved, used by dashboard
    "CACHE_URL": 2,
    "STREAM_URL": 3,
    "QUEUE_URL": 4,
    "TEAM_MEMBERS_CACHE_URL": 5,
    "TEAM_PERMISSIONS_CACHE_URL": 6,
    "USER_PERMISSIONS_CACHE_URL": 7,
    "EVENTLOGS_URL": 8,
    "ACTIVE_USERS_URL": 9,
    "STATISTICS_URL": 10,
    "AUTHENTICATION_URL": 11,
}


class RedisSettings(pydantic.BaseSettings):
    REDIS_SSL_VERIFY_MODE_CERT_NONE: bool = False
    REDIS_CRYPTO_SECRET_CURRENT: pydantic.SecretStr = pydantic.Field(
        extra_env="CACHE_TOKEN_SECRET"
    )
    REDIS_CRYPTO_SECRET_OLD: pydantic.SecretStr | None = pydantic.Field(
        default=None, extra_env="CACHE_TOKEN_SECRET_OLD"
    )

    # Legacy on-premise url
    STORAGE_URL: types.RedisDSN | None = None
    DEFAULT_REDIS_URL: types.RedisDSN = pydantic.Field(
        default=types.RedisDSN.parse("redis://localhost:6379"), extra_env="STORAGE_URL"
    )
    ENV_STREAM_URL: types.RedisDSN | None = pydantic.Field(
        default=None, extra_env="STREAM_URL"
    )
    ENV_EVENTLOGS_URL: types.RedisDSN | None = pydantic.Field(
        default=None, extra_env="EVENTLOGS_URL"
    )
    ENV_QUEUE_URL: types.RedisDSN | None = pydantic.Field(
        default=None, extra_env="QUEUE_URL"
    )
    ENV_CACHE_URL: types.RedisDSN | None = pydantic.Field(
        default=None, extra_env=("LEGACY_CACHE_URL", "CACHE_URL")
    )
    ENV_TEAM_PERMISSIONS_CACHE_URL: types.RedisDSN | None = pydantic.Field(
        default=None, extra_env="TEAM_PERMISSIONS_CACHE_URL"
    )
    ENV_TEAM_MEMBERS_CACHE_URL: types.RedisDSN | None = pydantic.Field(
        default=None, extra_env="TEAM_MEMBERS_CACHE_URL"
    )
    ENV_USER_PERMISSIONS_CACHE_URL: types.RedisDSN | None = pydantic.Field(
        default=None, extra_env="USER_PERMISSIONS_CACHE_URL"
    )
    ENV_ACTIVE_USERS_URL: types.RedisDSN | None = pydantic.Field(
        default=None, extra_env="ACTIVE_USERS_URL"
    )
    ENV_STATISTICS_URL: types.RedisDSN | None = pydantic.Field(
        default=None, extra_env="STATISTICS_URL"
    )
    ENV_AUTHENTICATION_URL: types.RedisDSN | None = pydantic.Field(
        default=None, extra_env="AUTHENTICATION_URL"
    )

    def _get_redis_url(self, name: str) -> types.RedisDSN:
        from_env = typing.cast(types.RedisDSN | None, getattr(self, f"ENV_{name}"))
        if from_env is not None:
            return from_env

        # NOTE(sileht): on legacy on-premise installation, before things were
        # auto sharded in redis databases, STREAM/QUEUE/LEGACY_CACHE/... was in
        # the same db, so keep it as-is
        if self.STORAGE_URL and name in (
            "STREAM_URL",
            "QUEUE_URL",
            "ACTIVE_USERS_URL",
            "CACHE_URL",
        ):
            # Legacy on-premise url
            return self.STORAGE_URL

        return self._build_redis_url(REDIS_AUTO_DB_SHARDING_MAPPING[name])

    def _build_redis_url(self, db: int) -> types.RedisDSN:
        if self.DEFAULT_REDIS_URL.query and "db" in parse.parse_qs(
            self.DEFAULT_REDIS_URL.query
        ):
            print(
                "DEFAULT_REDIS_URL must not contain any db parameter. Mergify can't start."
            )
            sys.exit(1)

        query = self.DEFAULT_REDIS_URL.query
        if query:
            query += "&"
        query += f"db={db}"

        return types.RedisDSN(
            scheme=self.DEFAULT_REDIS_URL.scheme,
            netloc=self.DEFAULT_REDIS_URL.netloc,
            path=self.DEFAULT_REDIS_URL.path,
            query=query,
            fragment=self.DEFAULT_REDIS_URL.fragment,
        )

    @property
    def STREAM_URL(self) -> types.RedisDSN:
        return self._get_redis_url("STREAM_URL")

    @property
    def EVENTLOGS_URL(self) -> types.RedisDSN:
        return self._get_redis_url("EVENTLOGS_URL")

    @property
    def QUEUE_URL(self) -> types.RedisDSN:
        return self._get_redis_url("QUEUE_URL")

    @property
    def CACHE_URL(self) -> types.RedisDSN:
        return self._get_redis_url("CACHE_URL")

    @property
    def TEAM_PERMISSIONS_CACHE_URL(self) -> types.RedisDSN:
        return self._get_redis_url("TEAM_PERMISSIONS_CACHE_URL")

    @property
    def TEAM_MEMBERS_CACHE_URL(self) -> types.RedisDSN:
        return self._get_redis_url("TEAM_MEMBERS_CACHE_URL")

    @property
    def USER_PERMISSIONS_CACHE_URL(self) -> types.RedisDSN:
        return self._get_redis_url("USER_PERMISSIONS_CACHE_URL")

    @property
    def ACTIVE_USERS_URL(self) -> types.RedisDSN:
        return self._get_redis_url("ACTIVE_USERS_URL")

    @property
    def STATISTICS_URL(self) -> types.RedisDSN:
        return self._get_redis_url("STATISTICS_URL")

    @property
    def AUTHENTICATION_URL(self) -> types.RedisDSN:
        return self._get_redis_url("AUTHENTICATION_URL")


class LogsSettings(pydantic.BaseSettings):
    LOG_LEVEL: types.LogLevel = types.LogLevel("INFO")
    LOG_STDOUT: bool = True
    LOG_STDOUT_LEVEL: types.LogLevel | None = None
    LOG_DATADOG: bool | pydantic.AnyHttpUrl = False
    LOG_DATADOG_LEVEL: types.LogLevel | None = None
    LOG_DEBUG_LOGGER_NAMES: list[str] = pydantic.Field(default_factory=list)
    SENTRY_URL: types.SecretUrl | None = None
    SENTRY_ENVIRONMENT: str = "test"


class GitHubSettings(pydantic.BaseSettings):
    GITHUB_URL: types.NormalizedUrl = types.NormalizedUrl.build(
        scheme="https", host="github.com"
    )
    GITHUB_APP_ID: int = pydantic.Field(extra_env="INTEGRATION_ID")
    GITHUB_PRIVATE_KEY: SecretStrFromBase64 = pydantic.Field(extra_env="PRIVATE_KEY")
    GITHUB_OAUTH_CLIENT_ID: str = pydantic.Field(extra_env="OAUTH_CLIENT_ID")
    GITHUB_OAUTH_CLIENT_SECRET: pydantic.SecretStr = pydantic.Field(
        extra_env="OAUTH_CLIENT_SECRET"
    )
    GITHUB_WEBHOOK_SECRET: pydantic.SecretStr = pydantic.Field(
        extra_env="WEBHOOK_SECRET"
    )
    GITHUB_WEBHOOK_SECRET_PRE_ROTATION: pydantic.SecretStr | None = pydantic.Field(
        default=None, extra_env="WEBHOOK_SECRET_PRE_ROTATION"
    )
    GITHUB_WEBHOOK_FORWARD_URL: str | None = pydantic.Field(
        default=None, extra_env="WEBHOOK_APP_FORWARD_URL"
    )
    GITHUB_WEBHOOK_FORWARD_EVENT_TYPES: list[str] = pydantic.Field(
        default_factory=list, extra_env="WEBHOOK_FORWARD_EVENT_TYPES"
    )

    @property
    def GITHUB_REST_API_URL(self) -> str:
        if self.GITHUB_URL.host == "github.com":
            return "https://api.github.com"
        return f"{self.GITHUB_URL}/api/v3"

    @property
    def GITHUB_GRAPHQL_API_URL(self) -> str:
        if self.GITHUB_URL.host == "github.com":
            return "https://api.github.com/graphql"
        return f"{self.GITHUB_URL}/api/graphql"


class DashboardUISettings(pydantic.BaseSettings):
    DASHBOARD_UI_STATIC_FILES_DIRECTORY: pydantic.DirectoryPath | None = None
    DASHBOARD_UI_FRONT_URL: pydantic.AnyHttpUrl = pydantic.Field(
        default=DASHBOARD_DEFAULT_URL,
        extra_env=("DASHBOARD_UI_FRONT_BASE_URL", "BASE_URL"),
    )
    DASHBOARD_UI_SESSION_EXPIRATION_HOURS: int = 24
    DASHBOARD_UI_FEATURES: list[str] = pydantic.Field(default_factory=list)
    DASHBOARD_UI_DATADOG_CLIENT_TOKEN: str | None = None
    DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO: list[int] = pydantic.Field(
        default_factory=list
    )


class WorkerSettings(pydantic.BaseSettings):
    SHARED_STREAM_PROCESSES: int = 1
    DEDICATED_STREAM_PROCESSES: int = 1
    SHARED_STREAM_TASKS_PER_PROCESS: int = 7
    BUCKET_PROCESSING_MAX_SECONDS: int = 30
    MAX_GITTER_CONCURRENT_JOBS: int = 20


class APISettings(pydantic.BaseSettings):
    API_ENABLE: bool = False
    REDIS_STREAM_WEB_MAX_CONNECTIONS: int = 50
    REDIS_CACHE_WEB_MAX_CONNECTIONS: int = 50
    REDIS_QUEUE_WEB_MAX_CONNECTIONS: int = 50
    REDIS_EVENTLOGS_WEB_MAX_CONNECTIONS: int = 50
    REDIS_STATS_WEB_MAX_CONNECTIONS: int = 50
    REDIS_ACTIVE_USERS_WEB_MAX_CONNECTIONS: int = 50
    REDIS_AUTHENTICATION_WEB_MAX_CONNECTIONS: int = 50


class SubscriptionSetting(pydantic.BaseSettings):
    SAAS_MODE: bool = False
    SUBSCRIPTION_URL: str = pydantic.Field(
        default="https://subscription.mergify.com", extra_env="SUBSCRIPTION_BASE_URL"
    )
    ENGINE_TO_DASHBOARD_API_KEY: pydantic.SecretStr = pydantic.Field(
        default=pydantic.SecretStr(secrets.token_hex(16)),
    )
    SUBSCRIPTION_TOKEN: pydantic.SecretStr | None = pydantic.Field(default=None)

    DASHBOARD_TO_ENGINE_API_KEY: pydantic.SecretStr = pydantic.Field(
        default=pydantic.SecretStr(secrets.token_hex(16)),
    )
    DASHBOARD_TO_ENGINE_API_KEY_PRE_ROTATION: pydantic.SecretStr | None = (
        pydantic.Field(
            default=None,
        )
    )

    ACCOUNT_TOKENS: list[tuple[int, str, pydantic.SecretStr]] = pydantic.Field(
        default_factory=list
    )
    APPLICATION_APIKEYS: dict[str, types.ApplicationAPIKey] = pydantic.Field(
        default_factory=dict
    )


class TestingSettings(pydantic.BaseSettings):
    TESTING_FORWARDER_ENDPOINT: str = "https://test-forwarder.mergify.io"
    TESTING_INSTALLATION_ID: int = 15398551
    TESTING_ORGANIZATION_ID: github_types.GitHubAccountIdType = (
        github_types.GitHubAccountIdType(40527191)
    )
    TESTING_ORGANIZATION_NAME: github_types.GitHubLogin = github_types.GitHubLogin(
        "mergifyio-testing"
    )
    TESTING_REPOSITORY_ID: github_types.GitHubRepositoryIdType = (
        github_types.GitHubRepositoryIdType(258840104)
    )
    TESTING_REPOSITORY_NAME: github_types.GitHubRepositoryName = (
        github_types.GitHubRepositoryName("functional-testing-repo")
    )
    TESTING_ORG_ADMIN_ID: github_types.GitHubAccountIdType = pydantic.Field(
        default=github_types.GitHubAccountIdType(38494943), extra_env="ORG_ADMIN_ID"
    )
    TESTING_ORG_ADMIN_PERSONAL_TOKEN: github_types.GitHubOAuthToken = pydantic.Field(
        default=github_types.GitHubOAuthToken(""), extra_env="ORG_ADMIN_PERSONAL_TOKEN"
    )
    TESTING_EXTERNAL_USER_PERSONAL_TOKEN: github_types.GitHubOAuthToken = (
        pydantic.Field(
            default=github_types.GitHubOAuthToken(""),
            extra_env="EXTERNAL_USER_PERSONAL_TOKEN",
        )
    )
    TESTING_ORG_USER_ID: github_types.GitHubAccountIdType = pydantic.Field(
        default=github_types.GitHubAccountIdType(74646794), extra_env="ORG_USER_ID"
    )
    TESTING_ORG_USER_PERSONAL_TOKEN: github_types.GitHubOAuthToken = pydantic.Field(
        default=github_types.GitHubOAuthToken(""), extra_env="ORG_USER_PERSONAL_TOKEN"
    )
    TESTING_MERGIFY_TEST_1_ID: github_types.GitHubAccountIdType = (
        github_types.GitHubAccountIdType(38494943)
    )
    TESTING_MERGIFY_TEST_2_ID: github_types.GitHubAccountIdType = (
        github_types.GitHubAccountIdType(38495008)
    )
    TESTING_GPGKEY_SECRET: str = ""
    TESTING_ID_GPGKEY_SECRET: str = ""
    TESTING_DEV_PERSONAL_TOKEN: str | None = pydantic.Field(
        default=None, extra_env="DEV_PERSONAL_TOKEN"
    )


class EngineSettings(
    APISettings,
    DatabaseSettings,
    LogsSettings,
    RedisSettings,
    GitHubSettings,
    DashboardUISettings,
    SubscriptionSetting,
    WorkerSettings,
    TestingSettings,
    pydantic.BaseSettings,
):
    VERSION: str = pydantic.Field("dev", extra_env="HEROKU_SLUG_COMMIT")
    SHA: str = "unknown"

    ALLOW_QUEUE_PRIORITY_ATTRIBUTE: bool = True
    HEALTHCHECK_SHARED_TOKEN: pydantic.SecretStr | None = None

    class Config(pydantic.BaseSettings.Config):
        case_sensitive = True
        env_prefix = "MERGIFYENGINE_"
        env_file = CONFIGURATION_FILE

        @classmethod
        def parse_env_var(cls, field_name: str, raw_val: str) -> typing.Any:
            if field_name == "DATABASE_POOL_SIZES":
                return utils.string_to_dict(raw_val, int)

            if field_name in (
                "GITHUB_WEBHOOK_FORWARD_EVENT_TYPES",
                "WEBHOOK_FORWARD_EVENT_TYPES",
                "DASHBOARD_UI_FEATURES",
                "DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO",
                "LOG_DEBUG_LOGGER_NAMES",
            ):
                return raw_val.split(",")

            if field_name == "APPLICATION_APIKEYS":
                return types.ApplicationAPIKeys(raw_val)

            if field_name == "ACCOUNT_TOKENS":
                return types.AccountTokens(raw_val)

            return super().parse_env_var(field_name, raw_val)

        @classmethod
        def prepare_field(cls, field: pydantic.fields.ModelField) -> None:
            # NOTE(sileht): pydantic-settings doesn't inject env_prefix on
            # `env` attribute, so we introduces extra_env to do it.
            # https://github.com/pydantic/pydantic-settings/issues/14

            extra_env = field.field_info.extra.get("extra_env")
            if extra_env:
                env_names = [cls.env_prefix + field.name]
                env = field.field_info.extra.get("env")
                if env is None:
                    pass
                elif isinstance(env, str):
                    env_names.append(env)
                # NOTE(sileht): we support only list as order matter
                elif isinstance(env, list | tuple):
                    env_names.extend(env)
                else:
                    raise RuntimeError(f"Unsupport env type: {type(env)}")

                if isinstance(extra_env, str):
                    env_names.append(cls.env_prefix + extra_env)

                # NOTE(sileht): we support only list as order matter
                elif isinstance(extra_env, list | tuple):
                    env_names.extend(
                        [
                            cls.env_prefix + extra_env_item
                            for extra_env_item in extra_env
                        ]
                    )
                else:
                    raise RuntimeError(f"Unsupport extra_env type: {type(extra_env)}")

                field.field_info.extra["env"] = env_names

            super().prepare_field(field)

    def __init__(self, **kwargs: typing.Any) -> None:
        try:
            super().__init__(**kwargs)
        except pydantic.ValidationError as exc:
            # Inject env_prefix in error message
            env_prefix = exc.model.__config__.env_prefix  # type: ignore[union-attr]
            for errors in exc.raw_errors:
                if not isinstance(errors, collections.abc.Sequence):
                    errors = [errors]
                for error in errors:
                    locs = error.loc_tuple()
                    loc = str.upper(env_prefix + locs[0])
                    error._loc = (loc,) + locs[1:]
            raise
