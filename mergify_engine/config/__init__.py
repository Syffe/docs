import datetime
import os
import secrets
import sys
import typing
from urllib import parse

import pydantic
import pydantic.fields
import pydantic_core
import pydantic_settings

from mergify_engine import github_types
from mergify_engine.config import types


# ################
# XXX: This is an ugly hack to hide all the "urls" from the pydantic
# errors so it doesn't appear in the json response of our endpoints.

real_errors = pydantic_core.ValidationError.errors


def errors_without_url(self, **kwargs) -> list[pydantic_core.ErrorDetails]:  # type: ignore[no-untyped-def]
    kwargs["include_url"] = False
    return real_errors(self, **kwargs)


pydantic_core.ValidationError.errors = errors_without_url  # type: ignore[method-assign]

# ################

CONFIGURATION_FILE = os.getenv("MERGIFYENGINE_TEST_SETTINGS")

DASHBOARD_DEFAULT_URL = "http://localhost:3000"


class DatabaseSettings(pydantic_settings.BaseSettings):
    DATABASE_URL: types.PostgresDSN = pydantic.Field(
        default=types.PostgresDSN.parse("postgres://localhost:5432"),
    )

    DATABASE_POOL_SIZES: types.StrIntDictFromStr = pydantic.Field(
        default=types.StrIntDictFromStr.from_dict({"worker": 15, "web": 55}),
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
    "ACTIVE_USERS_URL": 9,
    "STATISTICS_URL": 10,
    "AUTHENTICATION_URL": 11,
}


class RedisSettings(pydantic_settings.BaseSettings):
    REDIS_SSL_VERIFY_MODE_CERT_NONE: bool = False
    REDIS_CRYPTO_SECRET_CURRENT: pydantic.SecretStr = pydantic.Field(
        validation_alias=pydantic.AliasChoices(
            "REDIS_CRYPTO_SECRET_CURRENT",
            "CACHE_TOKEN_SECRET",
        ),
    )
    REDIS_CRYPTO_SECRET_OLD: pydantic.SecretStr | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices(
            "REDIS_CRYPTO_SECRET_OLD",
            "CACHE_TOKEN_SECRET_OLD",
        ),
    )

    # Legacy on-premise url
    STORAGE_URL: types.RedisDSN | None = None
    DEFAULT_REDIS_URL: types.RedisDSN = pydantic.Field(
        default=types.RedisDSN.parse("redis://localhost:6379"),
        validation_alias=pydantic.AliasChoices("DEFAULT_REDIS_URL", "STORAGE_URL"),
    )
    ENV_STREAM_URL: types.RedisDSN | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices("ENV_STREAM_URL", "STREAM_URL"),
    )
    ENV_QUEUE_URL: types.RedisDSN | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices("ENV_QUEUE_URL", "QUEUE_URL"),
    )
    ENV_CACHE_URL: types.RedisDSN | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices(
            "ENV_CACHE_URL",
            "CACHE_URL",
            "LEGACY_CACHE_URL",
        ),
    )
    ENV_TEAM_PERMISSIONS_CACHE_URL: types.RedisDSN | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices(
            "ENV_TEAM_PERMISSIONS_CACHE_URL",
            "TEAM_PERMISSIONS_CACHE_URL",
        ),
    )
    ENV_TEAM_MEMBERS_CACHE_URL: types.RedisDSN | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices(
            "ENV_TEAM_MEMBERS_CACHE_URL",
            "TEAM_MEMBERS_CACHE_URL",
        ),
    )
    ENV_USER_PERMISSIONS_CACHE_URL: types.RedisDSN | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices(
            "ENV_USER_PERMISSIONS_CACHE_URL",
            "USER_PERMISSIONS_CACHE_URL",
        ),
    )
    ENV_ACTIVE_USERS_URL: types.RedisDSN | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices(
            "ENV_ACTIVE_USERS_URL",
            "ACTIVE_USERS_URL",
        ),
    )
    ENV_STATISTICS_URL: types.RedisDSN | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices("ENV_STATISTICS_URL", "STATISTICS_URL"),
    )
    ENV_AUTHENTICATION_URL: types.RedisDSN | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices(
            "ENV_AUTHENTICATION_URL",
            "AUTHENTICATION_URL",
        ),
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
            self.DEFAULT_REDIS_URL.query,
        ):
            print(  # noqa: T201
                "DEFAULT_REDIS_URL must not contain any db parameter. Mergify can't start.",
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


class LogsSettings(pydantic_settings.BaseSettings):
    LOG_LEVEL: types.LogLevel = pydantic.Field(default="INFO", validate_default=True)  # type: ignore[assignment]
    LOG_STDOUT: bool = True
    LOG_STDOUT_LEVEL: types.LogLevel | None = None
    LOG_DATADOG: bool | types.UdpUrl = False
    LOG_DATADOG_LEVEL: types.LogLevel | None = None
    LOG_DEBUG_LOGGER_NAMES: types.StrListFromStrWithComma = (
        types.StrListFromStrWithComma([])
    )
    SENTRY_URL: types.SecretUrl | None = None
    SENTRY_ENVIRONMENT: str = "test"


class GitHubSettings(pydantic_settings.BaseSettings):
    GITHUB_URL: types.NormalizedUrl = pydantic.Field(  # type: ignore[assignment]
        default="https://github.com",
        validate_default=True,
    )
    GITHUB_APP_ID: int = pydantic.Field(
        validation_alias=pydantic.AliasChoices("GITHUB_APP_ID", "INTEGRATION_ID"),
    )
    GITHUB_PRIVATE_KEY: types.SecretStrFromBase64 = pydantic.Field(
        validation_alias=pydantic.AliasChoices("GITHUB_PRIVATE_KEY", "PRIVATE_KEY"),
    )
    GITHUB_OAUTH_CLIENT_ID: str = pydantic.Field(
        validation_alias=pydantic.AliasChoices(
            "GITHUB_OAUTH_CLIENT_ID",
            "OAUTH_CLIENT_ID",
        ),
    )
    GITHUB_OAUTH_CLIENT_SECRET: pydantic.SecretStr = pydantic.Field(
        validation_alias=pydantic.AliasChoices(
            "GITHUB_OAUTH_CLIENT_SECRET",
            "OAUTH_CLIENT_SECRET",
        ),
    )
    GITHUB_WEBHOOK_SECRET: pydantic.SecretStr = pydantic.Field(
        validation_alias=pydantic.AliasChoices(
            "GITHUB_WEBHOOK_SECRET",
            "WEBHOOK_SECRET",
        ),
    )
    GITHUB_WEBHOOK_SECRET_PRE_ROTATION: pydantic.SecretStr | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices(
            "GITHUB_WEBHOOK_SECRET_PRE_ROTATION",
            "WEBHOOK_SECRET_PRE_ROTATION",
        ),
    )
    GITHUB_WEBHOOK_FORWARD_URL: str | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices(
            "GITHUB_WEBHOOK_FORWARD_URL",
            "WEBHOOK_APP_FORWARD_URL",
        ),
    )
    GITHUB_WEBHOOK_FORWARD_EVENT_TYPES: types.StrListFromStrWithComma = pydantic.Field(
        default=types.StrListFromStrWithComma([]),
        validation_alias=pydantic.AliasChoices(
            "GITHUB_WEBHOOK_FORWARD_EVENT_TYPES",
            "WEBHOOK_FORWARD_EVENT_TYPES",
        ),
    )

    @property
    def IS_GHES(self) -> bool:
        return self.GITHUB_URL.host != "github.com"  # type: ignore[no-any-return]

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

    @property
    def GITHUB_REST_API_HOST(self) -> str:
        if self.GITHUB_URL.host == "github.com":
            return "api.github.com"
        return self.GITHUB_URL.host  # type: ignore[no-any-return]


class DashboardUISettings(pydantic_settings.BaseSettings):
    DASHBOARD_UI_STATIC_FILES_DIRECTORY: pydantic.DirectoryPath | None = None
    DASHBOARD_UI_FRONT_URL: types.NormalizedUrl = pydantic.Field(  # type: ignore[assignment]
        default=DASHBOARD_DEFAULT_URL,
        validate_default=True,
        validation_alias=pydantic.AliasChoices(
            "DASHBOARD_UI_FRONT_URL",
            "DASHBOARD_UI_FRONT_BASE_URL",
            "BASE_URL",
        ),
    )
    DASHBOARD_UI_SESSION_EXPIRATION_HOURS: int = 24
    DASHBOARD_UI_FEATURES: types.StrListFromStrWithComma = (
        types.StrListFromStrWithComma([])
    )
    DASHBOARD_UI_DATADOG_CLIENT_TOKEN: str | None = None
    DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO: types.IntListFromStrWithComma = (
        types.IntListFromStrWithComma([])
    )


class WorkerSettings(pydantic_settings.BaseSettings):
    SHARED_STREAM_PROCESSES: int = 1
    DEDICATED_STREAM_PROCESSES: int = 1
    SHARED_STREAM_TASKS_PER_PROCESS: int = 7
    BUCKET_PROCESSING_MAX_SECONDS: int = 30
    MAX_GITTER_CONCURRENT_JOBS: int = 20
    CI_DOWNLOAD_FREQUENCY: datetime.timedelta = datetime.timedelta(hours=1)
    CI_EVENT_PROCESSING_BATCH_SIZE: int = 1000
    # FIXME: stop injecting events until we reenable the consuming part
    CI_EVENT_INGESTION: bool = False
    CI_DOWNLOAD_BATCH_SIZE: int = 50
    GITHUB_IN_POSTGRES_EVENTS_INGESTION: bool = False
    GITHUB_IN_POSTGRES_PROCESSING_BATCH_SIZE: int = 1000
    GITHUB_IN_POSTGRES_USE_PR_IN_PG_FOR_ORGS: types.StrListFromStrWithComma = (
        types.StrListFromStrWithComma([])
    )


class APISettings(pydantic_settings.BaseSettings):
    API_ENABLE: bool = False
    REDIS_STREAM_WEB_MAX_CONNECTIONS: int = 50
    REDIS_CACHE_WEB_MAX_CONNECTIONS: int = 50
    REDIS_QUEUE_WEB_MAX_CONNECTIONS: int = 50
    REDIS_STATS_WEB_MAX_CONNECTIONS: int = 50
    REDIS_ACTIVE_USERS_WEB_MAX_CONNECTIONS: int = 50
    REDIS_AUTHENTICATION_WEB_MAX_CONNECTIONS: int = 50


class HTTPSettings(pydantic_settings.BaseSettings):
    HTTP_TRUSTED_HOSTS: types.StrListFromStrWithComma = types.StrListFromStrWithComma(
        ["*"],
    )
    HTTP_TO_HTTPS_REDIRECT: bool = True
    HTTP_CF_TO_MERGIFY_SECRET: pydantic.SecretStr | None = pydantic.Field(default=None)
    HTTP_CF_TO_MERGIFY_HOSTS: types.StrListFromStrWithComma = (
        types.StrListFromStrWithComma([])
    )
    HTTP_GITHUB_TO_MERGIFY_HOST: str = "github-webhook.mergify.com"
    HTTP_SAAS_SECURITY_ENFORCE: bool = False


class SubscriptionSetting(pydantic_settings.BaseSettings):
    SAAS_MODE: bool = False
    SUBSCRIPTION_URL: str = pydantic.Field(
        default="https://subscription.mergify.com",
        validation_alias=pydantic.AliasChoices(
            "SUBSCRIPTION_URL",
            "SUBSCRIPTION_BASE_URL",
        ),
    )
    ENGINE_TO_SHADOW_OFFICE_API_KEY: pydantic.SecretStr = pydantic.Field(
        default=pydantic.SecretStr(secrets.token_hex(16)),
        validation_alias=pydantic.AliasChoices(
            "ENGINE_TO_SHADOW_OFFICE_API_KEY",
            "ENGINE_TO_DASHBOARD_API_KEY",
        ),
    )
    SUBSCRIPTION_TOKEN: pydantic.SecretStr | None = pydantic.Field(default=None)

    SHADOW_OFFICE_TO_ENGINE_API_KEY: pydantic.SecretStr = pydantic.Field(
        default=pydantic.SecretStr(secrets.token_hex(16)),
        validation_alias=pydantic.AliasChoices(
            "SHADOW_OFFICE_TO_ENGINE_API_KEY",
            "DASHBOARD_TO_ENGINE_API_KEY",
        ),
    )
    SHADOW_OFFICE_TO_ENGINE_API_KEY_PRE_ROTATION: pydantic.SecretStr | None = (
        pydantic.Field(
            default=None,
            validation_alias=pydantic.AliasChoices(
                "SHADOW_OFFICE_TO_ENGINE_API_KEY_PRE_ROTATION",
                "DASHBOARD_TO_ENGINE_API_KEY_PRE_ROTATION",
            ),
        )
    )

    ACCOUNT_TOKENS: types.AccountTokens = pydantic.Field(
        default=types.AccountTokens([]),
        validate_default=True,
    )
    APPLICATION_APIKEYS: dict[str, types.ApplicationAPIKey] = pydantic.Field(
        default_factory=dict,
    )


class TestingSettings(pydantic_settings.BaseSettings):
    TESTING_RECORD: bool = False
    TESTING_RECORD_EVENTS_WAITING_TIME: int = 30
    TESTING_FORWARDER_ENDPOINT: str = "https://test-forwarder.mergify.com"
    TESTING_INSTALLATION_ID: int = 15398551
    TESTING_ORGANIZATION_ID: github_types.GitHubAccountIdType = (
        github_types.GitHubAccountIdType(40527191)
    )
    TESTING_ORGANIZATION_NAME: github_types.GitHubLogin = github_types.GitHubLogin(
        "mergifyio-testing",
    )
    TESTING_REPOSITORY_ID: github_types.GitHubRepositoryIdType = (
        github_types.GitHubRepositoryIdType(258840104)
    )
    TESTING_REPOSITORY_NAME: github_types.GitHubRepositoryName = (
        github_types.GitHubRepositoryName("functional-testing-repo")
    )
    TESTING_ORG_ADMIN_ID: github_types.GitHubAccountIdType = pydantic.Field(
        default=github_types.GitHubAccountIdType(38494943),
        validation_alias=pydantic.AliasChoices("TESTING_ORG_ADMIN_ID", "ORG_ADMIN_ID"),
    )
    TESTING_ORG_ADMIN_PERSONAL_TOKEN: github_types.GitHubOAuthToken = pydantic.Field(
        default=github_types.GitHubOAuthToken(""),
        validation_alias=pydantic.AliasChoices(
            "TESTING_ORG_ADMIN_PERSONAL_TOKEN",
            "ORG_ADMIN_PERSONAL_TOKEN",
        ),
    )
    TESTING_EXTERNAL_USER_PERSONAL_TOKEN: github_types.GitHubOAuthToken = (
        pydantic.Field(
            default=github_types.GitHubOAuthToken(""),
            validation_alias=pydantic.AliasChoices(
                "TESTING_EXTERNAL_USER_PERSONAL_TOKEN",
                "EXTERNAL_USER_PERSONAL_TOKEN",
            ),
        )
    )
    TESTING_ORG_USER_ID: github_types.GitHubAccountIdType = pydantic.Field(
        default=github_types.GitHubAccountIdType(74646794),
        validation_alias=pydantic.AliasChoices("TESTING_ORG_USER_ID", "ORG_USER_ID"),
    )
    TESTING_ORG_USER_PERSONAL_TOKEN: github_types.GitHubOAuthToken = pydantic.Field(
        default=github_types.GitHubOAuthToken(""),
        validation_alias=pydantic.AliasChoices(
            "TESTING_ORG_USER_PERSONAL_TOKEN",
            "ORG_USER_PERSONAL_TOKEN",
        ),
    )
    TESTING_MERGIFY_TEST_1_ID: github_types.GitHubAccountIdType = (
        github_types.GitHubAccountIdType(38494943)
    )
    TESTING_MERGIFY_TEST_2_ID: github_types.GitHubAccountIdType = (
        github_types.GitHubAccountIdType(38495008)
    )
    TESTING_GPG_SECRET_KEY: str = """
--- nosemgrep: generic.secrets.security.detected-pgp-private-key-block.detected-pgp-private-key-block
-----BEGIN PGP PRIVATE KEY BLOCK-----

lFgEZL6cGRYJKwYBBAHaRw8BAQdACJKhmijinQdAw6EUsS/OOpR0TNio6dYTvdBm
0Gr+iI0AAPwP32IiJ3WaL4bD5QR1i6CG8XF3Q/5zSo/cbdAZXeselxDAtEpNZXJn
aWZ5IGVuZ2luZWVyaW5nIChtZXJnaWZ5LXRlc3QyKSA8ZW5naW5lZXJpbmcrbWVy
Z2lmeS10ZXN0QG1lcmdpZnkuY29tPoiTBBMWCgA7FiEEjHs1+K/aKS7ATmcadwUM
WNd6GJcFAmS+nBkCGwMFCwkIBwICIgIGFQoJCAsCBBYCAwECHgcCF4AACgkQdwUM
WNd6GJeWHQD/YgZid6Nc+Insb/Z0vFYBtXrp4Xg3tu0zVYGV6Kr6r00BAJi/5Bt3
0+S1Tq5RroRDwnurXoy1iqjjvAn8xcR9CaMKnF0EZL6cGRIKKwYBBAGXVQEFAQEH
QIznpvg6S4R+tf5fB4Mr8MfQBHkgaGg0TK6FamdqpeNnAwEIBwAA/1mUwqBybwWp
e2rG9NBUlfxYoQx3pBaz6W/78gkkR4z4EleIeAQYFgoAIBYhBIx7Nfiv2ikuwE5n
GncFDFjXehiXBQJkvpwZAhsMAAoJEHcFDFjXehiX3GoBAOB5aca4sBO/MrgnYYd4
3EMcwCvPDdnJOuAPuZvQUJ5+AQDVXtWOfig+zKwnFgFNk/HvqI7wiFUSOvzlwNTY
wEb0Bg==
=3wUc
-----END PGP PRIVATE KEY BLOCK-----
"""
    TESTING_GPG_SECRET_KEY_ID: str = "77050C58D77A1897"
    TESTING_DEV_PERSONAL_TOKEN: pydantic.SecretStr | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices(
            "TESTING_DEV_PERSONAL_TOKEN",
            "DEV_PERSONAL_TOKEN",
        ),
    )


class LogEmbedderSettings(pydantic_settings.BaseSettings):
    OPENAI_API_TOKEN: pydantic.SecretStr = pydantic.Field(
        default=pydantic.SecretStr(""),
    )
    LOG_EMBEDDER_METADATA_EXTRACT_MODEL: types.OpenAIModel = "gpt-4-1106-preview"
    LOG_EMBEDDER_ENABLED_ORGS: types.GitHubLoginListFromStrWithComma = (
        types.GitHubLoginListFromStrWithComma([])
    )
    LOG_EMBEDDER_GCS_BUCKET: str = pydantic.Field(default="mergify-ci-monitoring-logs")
    LOG_EMBEDDER_GCS_CREDENTIALS: types.SecretStrFromBase64 | None = pydantic.Field(
        default=None,
    )


def _extract_field_info(
    self: pydantic_settings.EnvSettingsSource,
    field: pydantic.fields.FieldInfo,
    field_name: str,
) -> list[tuple[str, str, bool]]:
    """
    Overload of `PydanticBaseEnvSettingsSource._extract_field_info` to allow
    `env_prefix` to be put before the aliases.

    Extracts field info. This info is used to get the value of field from environment variables.

    It returns a list of tuples, each tuple contains:
        * field_key: The key of field that has to be used in model creation.
        * env_name: The environment variable name of the field.
        * value_is_complex: A flag to determine whether the value from environment variable
          is complex and has to be parsed.

    Args:
        field (FieldInfo): The field.
        field_name (str): The field name.

    Returns:
        list[tuple[str, str, bool]]: List of tuples, each tuple contains field_key, env_name, and value_is_complex.
    """
    field_info: list[tuple[str, str, bool]] = []
    if isinstance(field.validation_alias, pydantic.AliasChoices | pydantic.AliasPath):
        v_alias: str | list[str | int] | list[
            list[str | int]
        ] | None = field.validation_alias.convert_to_aliases()
    else:
        v_alias = field.validation_alias

    if v_alias:
        if isinstance(v_alias, list):  # AliasChoices, AliasPath
            for alias in v_alias:
                if isinstance(alias, str):  # AliasPath
                    field_info.append(
                        (
                            alias,
                            self._apply_case_sensitive(self.env_prefix + alias),
                            len(alias) > 1,
                        ),
                    )
                elif isinstance(alias, list):  # AliasChoices
                    first_arg = typing.cast(
                        str,
                        alias[0],
                    )  # first item of an AliasChoices must be a str
                    field_info.append(
                        (
                            first_arg,
                            self._apply_case_sensitive(self.env_prefix + first_arg),
                            len(alias) > 1,
                        ),
                    )
        else:  # string validation alias
            field_info.append(
                (
                    v_alias,
                    self._apply_case_sensitive(self.env_prefix + v_alias),
                    False,
                ),
            )
    else:
        field_info.append(
            (
                field_name,
                self._apply_case_sensitive(self.env_prefix + field_name),
                False,
            ),
        )

    return field_info


def _prepare_field_value(
    self: pydantic_settings.EnvSettingsSource,
    field_name: str,
    field: pydantic.fields.FieldInfo,
    value: typing.Any,
    value_is_complex: bool,
) -> typing.Any:
    if field_name == "APPLICATION_APIKEYS":
        return types.ApplicationAPIKeys(value or "")

    return value


class DotEnvEngineSettingsSource(pydantic_settings.DotEnvSettingsSource):
    prepare_field_value = _prepare_field_value
    _extract_field_info = _extract_field_info


class EnvEngineSettingsSource(pydantic_settings.EnvSettingsSource):
    prepare_field_value = _prepare_field_value
    _extract_field_info = _extract_field_info


class EngineSettings(
    APISettings,
    HTTPSettings,
    DatabaseSettings,
    LogsSettings,
    RedisSettings,
    GitHubSettings,
    DashboardUISettings,
    SubscriptionSetting,
    WorkerSettings,
    TestingSettings,
    LogEmbedderSettings,
    pydantic_settings.BaseSettings,
):
    VERSION: str = pydantic.Field(
        "dev",
        validation_alias=pydantic.AliasChoices("HEROKU_SLUG_COMMIT", "VERSION"),
    )
    SHA: str = "unknown"

    HEALTHCHECK_SHARED_TOKEN: pydantic.SecretStr | None = None

    ALLOW_REQUIRE_BRANCH_PROTECTION_QUEUE_ATTRIBUTE: bool = pydantic.Field(default=True)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[pydantic_settings.BaseSettings],
        init_settings: pydantic_settings.PydanticBaseSettingsSource,
        env_settings: pydantic_settings.PydanticBaseSettingsSource,
        dotenv_settings: pydantic_settings.PydanticBaseSettingsSource,
        file_secret_settings: pydantic_settings.PydanticBaseSettingsSource,
    ) -> tuple[pydantic_settings.PydanticBaseSettingsSource, ...]:
        # Order is taken into account, we load values from dotenv file first,
        # then from the env if not found in the dotenv file
        return (
            DotEnvEngineSettingsSource(settings_cls),
            EnvEngineSettingsSource(settings_cls),
        )

    model_config = pydantic_settings.SettingsConfigDict(
        case_sensitive=True,
        env_prefix="MERGIFYENGINE_",
        env_file=CONFIGURATION_FILE,
    )
