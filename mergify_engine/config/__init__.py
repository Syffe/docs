import base64
import logging
import os
import secrets
import sys
import typing
from urllib import parse

import dotenv
import pydantic
import voluptuous

from mergify_engine import github_types
from mergify_engine import utils
from mergify_engine.config import urls


CONFIGURATION_FILE = os.getenv("MERGIFYENGINE_TEST_SETTINGS")

DASHBOARD_DEFAULT_URL = pydantic.HttpUrl("http://localhost:8802", scheme="http")


class EngineSettings(pydantic.BaseSettings):
    DATABASE_URL: urls.PostgresDSN = urls.PostgresDSN.parse("postgres://localhost:5432")
    DATABASE_POOL_SIZES: dict[str, int] = pydantic.Field(
        default={"worker": 15, "web": 55}
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

    DASHBOARD_UI_STATIC_FILES_DIRECTORY: pydantic.DirectoryPath | None = None
    DASHBOARD_UI_FRONT_BASE_URL: pydantic.HttpUrl = pydantic.Field(
        default=DASHBOARD_DEFAULT_URL, extra_env="BASE_URL"
    )
    DASHBOARD_UI_SITE_URLS: list[pydantic.HttpUrl] = pydantic.Field(
        default=[DASHBOARD_DEFAULT_URL], extra_env="BASE_URL"
    )
    DASHBOARD_UI_SESSION_EXPIRATION_HOURS: int = 24
    DASHBOARD_UI_FEATURES: list[str] = pydantic.Field(default_factory=list)
    DASHBOARD_UI_DATADOG_CLIENT_TOKEN: str | None = None
    DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO: list[int] = pydantic.Field(
        default_factory=list
    )

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
                "DASHBOARD_UI_SITE_URLS",
                "DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO",
            ):
                return raw_val.split(",")

            return super().parse_env_var(field_name, raw_val)

        @classmethod
        def prepare_field(cls, field: pydantic.fields.ModelField) -> None:
            # NOTE(sileht): pydantic-settings doesn't inject env_prefix on
            # `env` attribute, so we introduces extra_env to do it.
            # https://github.com/pydantic/pydantic-settings/issues/14

            extra_env = field.field_info.extra.get("extra_env")
            if extra_env:
                env = field.field_info.extra.get("env")
                if env is None:
                    env_names = set()
                elif isinstance(env, str):
                    env_names = {env}
                elif isinstance(env, (set, tuple, list)):
                    env_names = set(env)
                else:
                    raise RuntimeError(f"Unsupport env type: {type(env)}")

                env_names |= {cls.env_prefix + field.name, cls.env_prefix + extra_env}
                field.field_info.extra["env"] = env_names

            super().prepare_field(field)

    def __init__(self, **kwargs: typing.Any) -> None:
        try:
            super().__init__(**kwargs)
        except pydantic.ValidationError as exc:
            # Inject env_prefix in error message
            cfg = exc.model.__config__  # type: ignore[union-attr]
            for e in exc.raw_errors:
                e._loc = tuple(str.upper(cfg.env_prefix + x) for x in e.loc_tuple())  # type: ignore[union-attr]

            raise


# NOTE(sileht) we coerce bool and int in case they are loaded from the environment
def CoercedBool(value: typing.Any) -> bool:
    return utils.strtobool(str(value))


def CoercedLoggingLevel(value: str) -> int:
    value = value.upper()
    if value in ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"):
        return int(getattr(logging, value))
    raise ValueError(value)


def CommaSeparatedStringList(value: str) -> list[str]:
    if value:
        return value.split(",")
    else:
        return []


def CommaSeparatedIntList(value: str) -> list[int]:
    return [int(v) for v in CommaSeparatedStringList(value)]


def AccountTokens(v: str) -> list[tuple[int, str, str]]:
    try:
        return [
            (int(_id), login, token)
            for _id, login, token in typing.cast(
                list[tuple[int, str, str]],
                utils.string_to_list_of_tuple(v, split=3),
            )
        ]
    except ValueError:
        raise ValueError("wrong format, expect `id1:login1:token1,id2:login2:token2`")


API_ACCESS_KEY_LEN = 32
API_SECRET_KEY_LEN = 32


class ApplicationAPIKey(typing.TypedDict):
    api_secret_key: str
    api_access_key: str
    account_id: int
    account_login: str


def ApplicationAPIKeys(v: str) -> dict[str, ApplicationAPIKey]:
    try:
        applications = utils.string_to_list_of_tuple(v, split=3)
        _validate_application_api_keys(applications)
    except ValueError:
        raise ValueError(
            "wrong format, "
            "expect `api_key1:github_account_id1:github_account_login1,api_key1:github_account_id2:github_account_login2`, "
            "api_key must be 64 character long"
        )
    else:
        return {
            api_key[:API_ACCESS_KEY_LEN]: {
                "api_access_key": api_key[:API_ACCESS_KEY_LEN],
                "api_secret_key": api_key[API_ACCESS_KEY_LEN:],
                "account_id": int(account_id),
                "account_login": account_login,
            }
            for api_key, account_id, account_login in applications
        }


def _validate_application_api_keys(applications: list[tuple[str, ...]]) -> None:
    for api_key, _, _ in applications:
        if len(api_key) != API_ACCESS_KEY_LEN + API_ACCESS_KEY_LEN:
            raise ValueError("api_key must be 64 character long")


Schema = voluptuous.Schema(
    {
        voluptuous.Required(
            "VERSION", default=os.getenv("HEROKU_SLUG_COMMIT", "dev")
        ): str,
        voluptuous.Required("SAAS_MODE", default=False): CoercedBool,
        # Logging
        voluptuous.Required(
            "LOG_DEBUG_LOGGER_NAMES", default=""
        ): CommaSeparatedStringList,
        voluptuous.Required("API_ENABLE", default=False): CoercedBool,
        voluptuous.Required("LOG_LEVEL", default="INFO"): CoercedLoggingLevel,
        voluptuous.Required("LOG_RATELIMIT", default=False): CoercedBool,
        voluptuous.Required("LOG_STDOUT", default=True): CoercedBool,
        voluptuous.Required("LOG_STDOUT_LEVEL", default=None): voluptuous.Any(
            None, CoercedLoggingLevel
        ),
        voluptuous.Required("LOG_DATADOG", default=False): voluptuous.Any(
            CoercedBool, voluptuous.Url
        ),
        voluptuous.Required("LOG_DATADOG_LEVEL", default=None): voluptuous.Any(
            None, CoercedLoggingLevel
        ),
        voluptuous.Required("SENTRY_URL", default=None): voluptuous.Any(None, str),
        voluptuous.Required("SENTRY_ENVIRONMENT", default="test"): str,
        # GitHub App mandatory
        voluptuous.Required("INTEGRATION_ID"): voluptuous.Coerce(int),
        voluptuous.Required("PRIVATE_KEY"): str,
        voluptuous.Required("OAUTH_CLIENT_ID"): str,
        voluptuous.Required("OAUTH_CLIENT_SECRET"): str,
        # GitHub optional
        voluptuous.Required("GITHUB_URL", default="https://github.com"): str,
        voluptuous.Required(
            "SUBSCRIPTION_BASE_URL", default="https://subscription.mergify.com"
        ): str,
        #
        # OnPremise special config
        #
        voluptuous.Required("SUBSCRIPTION_TOKEN", default=None): voluptuous.Any(
            None, str
        ),
        voluptuous.Required("ACCOUNT_TOKENS", default=""): voluptuous.Coerce(
            AccountTokens
        ),
        voluptuous.Required("APPLICATION_APIKEYS", default=""): voluptuous.Coerce(
            ApplicationAPIKeys
        ),
        # Saas Special config
        voluptuous.Required(
            "ENGINE_TO_DASHBOARD_API_KEY", default=secrets.token_hex(16)
        ): str,
        voluptuous.Required(
            "DASHBOARD_TO_ENGINE_API_KEY", default=secrets.token_hex(16)
        ): str,
        voluptuous.Required(
            "DASHBOARD_TO_ENGINE_API_KEY_PRE_ROTATION", default=None
        ): voluptuous.Any(None, str),
        #
        # Mergify Engine settings
        #
        voluptuous.Required("DATABASE_OAUTH_TOKEN_SECRET_CURRENT"): str,
        voluptuous.Required(
            "DATABASE_OAUTH_TOKEN_SECRET_OLD", default=None
        ): voluptuous.Any(None, str),
        voluptuous.Required(
            "REDIS_SSL_VERIFY_MODE_CERT_NONE", default=False
        ): CoercedBool,
        voluptuous.Required(
            "REDIS_STREAM_WEB_MAX_CONNECTIONS", default=50
        ): voluptuous.Coerce(int),
        voluptuous.Required(
            "REDIS_CACHE_WEB_MAX_CONNECTIONS", default=50
        ): voluptuous.Coerce(int),
        voluptuous.Required(
            "REDIS_QUEUE_WEB_MAX_CONNECTIONS", default=50
        ): voluptuous.Coerce(int),
        voluptuous.Required(
            "REDIS_EVENTLOGS_WEB_MAX_CONNECTIONS", default=50
        ): voluptuous.Coerce(int),
        voluptuous.Required(
            "REDIS_STATS_WEB_MAX_CONNECTIONS", default=50
        ): voluptuous.Coerce(int),
        voluptuous.Required(
            "REDIS_ACTIVE_USERS_WEB_MAX_CONNECTIONS", default=50
        ): voluptuous.Coerce(int),
        voluptuous.Required(
            "REDIS_AUTHENTICATION_WEB_MAX_CONNECTIONS", default=50
        ): voluptuous.Coerce(int),
        # NOTE(sileht): Unused anymore, but keep to detect legacy onpremise installation
        voluptuous.Required("STORAGE_URL", default=None): voluptuous.Any(None, str),
        # NOTE(sileht): Not used directly, but used to build other redis urls if not provided
        voluptuous.Required("DEFAULT_REDIS_URL", default=None): voluptuous.Any(
            None, str
        ),
        voluptuous.Required("LEGACY_CACHE_URL", default=None): voluptuous.Any(
            None, str
        ),
        voluptuous.Required("QUEUE_URL", default=None): voluptuous.Any(None, str),
        voluptuous.Required("STREAM_URL", default=None): voluptuous.Any(None, str),
        voluptuous.Required("TEAM_MEMBERS_CACHE_URL", default=None): voluptuous.Any(
            None, str
        ),
        voluptuous.Required("TEAM_PERMISSIONS_CACHE_URL", default=None): voluptuous.Any(
            None, str
        ),
        voluptuous.Required("USER_PERMISSIONS_CACHE_URL", default=None): voluptuous.Any(
            None, str
        ),
        voluptuous.Required("ACTIVE_USERS_URL", default=None): voluptuous.Any(
            None, str
        ),
        voluptuous.Required("EVENTLOGS_URL", default=None): voluptuous.Any(None, str),
        voluptuous.Required("STATISTICS_URL", default=None): voluptuous.Any(None, str),
        voluptuous.Required("AUTHENTICATION_URL", default=None): voluptuous.Any(
            None, str
        ),
        voluptuous.Required("SHARED_STREAM_PROCESSES", default=1): voluptuous.Coerce(
            int
        ),
        voluptuous.Required("DEDICATED_STREAM_PROCESSES", default=1): voluptuous.Coerce(
            int
        ),
        voluptuous.Required(
            "SHARED_STREAM_TASKS_PER_PROCESS", default=7
        ): voluptuous.Coerce(int),
        voluptuous.Required(
            "BUCKET_PROCESSING_MAX_SECONDS", default=30
        ): voluptuous.Coerce(int),
        voluptuous.Required(
            "MAX_GITTER_CONCURRENT_JOBS", default=20
        ): voluptuous.Coerce(int),
        voluptuous.Required("CACHE_TOKEN_SECRET"): str,
        voluptuous.Required("CACHE_TOKEN_SECRET_OLD", default=None): voluptuous.Any(
            None, str
        ),
        voluptuous.Required(
            "ALLOW_QUEUE_PRIORITY_ATTRIBUTE", default=True
        ): CoercedBool,
        # For test suite only (eg: tox -erecord)
        voluptuous.Required(
            "TESTING_FORWARDER_ENDPOINT",
            default="https://test-forwarder.mergify.io",
        ): str,
        voluptuous.Required(
            "TESTING_INSTALLATION_ID", default=15398551
        ): voluptuous.Coerce(int),
        voluptuous.Required(
            "TESTING_REPOSITORY_ID", default=258840104
        ): voluptuous.Coerce(int),
        voluptuous.Required(
            "TESTING_REPOSITORY_NAME", default="functional-testing-repo"
        ): str,
        voluptuous.Required(
            "TESTING_ORGANIZATION_ID", default=40527191
        ): voluptuous.Coerce(int),
        voluptuous.Required(
            "TESTING_ORGANIZATION_NAME", default="mergifyio-testing"
        ): str,
        voluptuous.Required("ORG_ADMIN_ID", default=38494943): int,
        voluptuous.Required(
            "ORG_ADMIN_PERSONAL_TOKEN",
            default="<ORG_ADMIN_PERSONAL_TOKEN>",
        ): str,
        voluptuous.Required(
            "EXTERNAL_USER_PERSONAL_TOKEN", default="<EXTERNAL_USER_TOKEN>"
        ): str,
        voluptuous.Required("ORG_USER_ID", default=74646794): int,
        voluptuous.Required("ORG_USER_PERSONAL_TOKEN", default="<ORG_USER_TOKEN>"): str,
        voluptuous.Required(
            "TESTING_MERGIFY_TEST_1_ID", default=38494943
        ): voluptuous.Coerce(int),
        voluptuous.Required(
            "TESTING_MERGIFY_TEST_2_ID", default=38495008
        ): voluptuous.Coerce(int),
        "TESTING_GPGKEY_SECRET": str,
        "TESTING_ID_GPGKEY_SECRET": str,
        voluptuous.Required("DEV_PERSONAL_TOKEN", default="<DEV_PERSONAL_TOKEN>"): str,
    }
)

# Config variables available from voluptuous
VERSION: str
API_ENABLE: bool
SENTRY_URL: str
SENTRY_ENVIRONMENT: str
CACHE_TOKEN_SECRET: str
CACHE_TOKEN_SECRET_OLD: str | None
PRIVATE_KEY: bytes
GITHUB_URL: str
GITHUB_REST_API_URL: str
GITHUB_GRAPHQL_API_URL: str
SHARED_STREAM_PROCESSES: int
DEDICATED_STREAM_PROCESSES: int
SHARED_STREAM_TASKS_PER_PROCESS: int
EXTERNAL_USER_PERSONAL_TOKEN: github_types.GitHubOAuthToken

DATABASE_OAUTH_TOKEN_SECRET_CURRENT: str
DATABASE_OAUTH_TOKEN_SECRET_OLD: str | None

STREAM_URL: str
EVENTLOGS_URL: str
QUEUE_URL: str
LEGACY_CACHE_URL: str
TEAM_PERMISSIONS_CACHE_URL: str
TEAM_MEMBERS_CACHE_URL: str
USER_PERMISSIONS_CACHE_URL: str
ACTIVE_USERS_URL: str
STATISTICS_URL: str
AUTHENTICATION_URL: str

BUCKET_PROCESSING_MAX_SECONDS: int
MAX_GITTER_CONCURRENT_JOBS: int
INTEGRATION_ID: int
SUBSCRIPTION_BASE_URL: str
SUBSCRIPTION_TOKEN: str | None
ENGINE_TO_DASHBOARD_API_KEY: str
DASHBOARD_TO_ENGINE_API_KEY: str
DASHBOARD_TO_ENGINE_API_KEY_PRE_ROTATION: str
OAUTH_CLIENT_ID: str
OAUTH_CLIENT_SECRET: str
ACCOUNT_TOKENS: list[tuple[int, str, str]]
APPLICATION_APIKEYS: dict[str, ApplicationAPIKey]
ALLOW_QUEUE_PRIORITY_ATTRIBUTE: bool
ALLOW_REBASE_FALLBACK_ATTRIBUTE: bool
REDIS_SSL_VERIFY_MODE_CERT_NONE: bool
REDIS_STREAM_WEB_MAX_CONNECTIONS: int | None
REDIS_CACHE_WEB_MAX_CONNECTIONS: int | None
REDIS_QUEUE_WEB_MAX_CONNECTIONS: int | None
REDIS_EVENTLOGS_WEB_MAX_CONNECTIONS: int | None
REDIS_STATS_WEB_MAX_CONNECTIONS: int | None
REDIS_AUTHENTICATION_WEB_MAX_CONNECTIONS: int | None
REDIS_ACTIVE_USERS_WEB_MAX_CONNECTIONS: int | None
TESTING_ORGANIZATION_ID: github_types.GitHubAccountIdType
TESTING_ORGANIZATION_NAME: github_types.GitHubLogin
TESTING_REPOSITORY_ID: github_types.GitHubRepositoryIdType
TESTING_REPOSITORY_NAME: str
TESTING_FORWARDER_ENDPOINT: str
LOG_LEVEL: int  # This is converted to an int by voluptuous
LOG_STDOUT: bool
LOG_STDOUT_LEVEL: int  # This is converted to an int by voluptuous
LOG_DATADOG: bool | str
LOG_DATADOG_LEVEL: int  # This is converted to an int by voluptuous
LOG_DEBUG_LOGGER_NAMES: list[str]
ORG_ADMIN_PERSONAL_TOKEN: github_types.GitHubOAuthToken
ORG_ADMIN_ID: github_types.GitHubAccountIdType
ORG_USER_ID: github_types.GitHubAccountIdType
ORG_USER_PERSONAL_TOKEN: github_types.GitHubOAuthToken
TESTING_MERGIFY_TEST_1_ID: github_types.GitHubAccountIdType
TESTING_MERGIFY_TEST_2_ID: github_types.GitHubAccountIdType
TESTING_GPGKEY_SECRET: bytes
TESTING_ID_GPGKEY_SECRET: str
TESTING_INSTALLATION_ID: github_types.GitHubAccountIdType
SAAS_MODE: bool
DEV_PERSONAL_TOKEN: github_types.GitHubOAuthToken

# config variables built
GITHUB_DOMAIN: str


def load() -> dict[str, typing.Any]:
    if CONFIGURATION_FILE is not None:
        dotenv.load_dotenv(dotenv_path=CONFIGURATION_FILE, override=True)

    raw_config: dict[str, typing.Any] = {}
    for key, _ in Schema.schema.items():
        val = os.getenv(f"MERGIFYENGINE_{key}")
        if val is not None:
            raw_config[key] = val

    parsed_config = Schema(raw_config)

    # NOTE(sileht): on legacy on-premise installation, before things were auto
    # sharded in redis databases, STREAM/QUEUE/LEGACY_CACHE was in the same db, so
    # keep until manual migration as been done
    if parsed_config["STORAGE_URL"] is not None:
        for config_key in (
            "DEFAULT_REDIS_URL",
            "STREAM_URL",
            "QUEUE_URL",
            "ACTIVE_USERS_URL",
            "LEGACY_CACHE_URL",
        ):
            if parsed_config[config_key] is None:
                parsed_config[config_key] = parsed_config["STORAGE_URL"]

    # NOTE(sileht): If we reach 15, we should update onpremise installation guide
    # and add an upgrade release note section to ensure people configure their Redis
    # correctly
    REDIS_AUTO_DB_SHARDING_MAPPING = {
        # 0 reserved, never use it, this force people to select a DB before running any command
        # and maybe be used by legacy onpremise installation.
        # 1 temporary reserved, used by dashboard
        "LEGACY_CACHE_URL": 2,
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

    default_redis_url_parsed = parse.urlparse(
        parsed_config["DEFAULT_REDIS_URL"] or "redis://localhost:6379"
    )
    if default_redis_url_parsed.query and "db" in parse.parse_qs(
        default_redis_url_parsed.query
    ):
        print(
            "DEFAULT_REDIS_URL must not contain any db parameter. Mergify can't start."
        )
        sys.exit(1)

    for config_key, db in REDIS_AUTO_DB_SHARDING_MAPPING.items():
        if parsed_config[config_key] is None:
            query = default_redis_url_parsed.query
            if query:
                query += "&"
            query += f"db={db}"
            url = default_redis_url_parsed._replace(query=query).geturl()
            parsed_config[config_key] = url

    parsed_config["GITHUB_DOMAIN"] = parse.urlparse(
        parsed_config["GITHUB_URL"]
    ).hostname

    # NOTE(sileht): Docker can't pass multiline in environment, so we allow to pass
    # it in base64 format
    if not parsed_config["PRIVATE_KEY"].startswith("----"):
        parsed_config["PRIVATE_KEY"] = base64.b64decode(parsed_config["PRIVATE_KEY"])

    if "TESTING_GPGKEY_SECRET" in parsed_config and not parsed_config[
        "TESTING_GPGKEY_SECRET"
    ].startswith("----"):
        parsed_config["TESTING_GPGKEY_SECRET"] = base64.b64decode(
            parsed_config["TESTING_GPGKEY_SECRET"]
        )

    if not parsed_config["SAAS_MODE"] and not parsed_config["SUBSCRIPTION_TOKEN"]:
        print("SUBSCRIPTION_TOKEN is missing. Mergify can't start.")
        sys.exit(1)

    parsed_config["GITHUB_URL"] = parsed_config["GITHUB_URL"].removesuffix("/")

    if parsed_config["GITHUB_URL"].startswith("https://github.com"):
        parsed_config["GITHUB_REST_API_URL"] = "https://api.github.com"
        parsed_config["GITHUB_GRAPHQL_API_URL"] = "https://api.github.com/graphql"
    else:
        parsed_config["GITHUB_REST_API_URL"] = f"{parsed_config['GITHUB_URL']}/api/v3"
        parsed_config[
            "GITHUB_GRAPHQL_API_URL"
        ] = f"{parsed_config['GITHUB_URL']}/api/graphql"

    return parsed_config  # type: ignore[no-any-return]


CONFIG = load()
globals().update(CONFIG)
