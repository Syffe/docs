import os

from datadog import statsd  # type: ignore[attr-defined]
import ddtrace
import sentry_sdk
from sentry_sdk.integrations import fastapi
from sentry_sdk.integrations import httpx
from sentry_sdk.integrations import sqlalchemy
from sentry_sdk.integrations import starlette

from mergify_engine import config
from mergify_engine import logs
from mergify_engine import models


SERVICE_NAME: str = "engine-<unknown>"
VERSION: str = os.environ.get("MERGIFYENGINE_SHA", "unknown")


def ddtrace_hook(span: ddtrace.Span) -> None:
    root_span = ddtrace.tracer.current_root_span()
    if root_span is None:
        return

    owner = root_span.get_tag("gh_owner")
    if owner and span.get_tag("gh_owner") is None:
        span.set_tag("gh_owner", owner)


def setup(service_name: str, dump_config: bool = True, pg_pool_size: int = 10) -> None:
    global SERVICE_NAME
    SERVICE_NAME = "engine-" + service_name

    if config.SENTRY_URL:  # pragma: no cover
        sentry_sdk.init(
            config.SENTRY_URL,
            max_breadcrumbs=10,
            release=VERSION,
            environment=config.SENTRY_ENVIRONMENT,
            request_bodies="never",
            integrations=[
                httpx.HttpxIntegration(),
                starlette.StarletteIntegration(),
                fastapi.FastApiIntegration(),
                sqlalchemy.SqlalchemyIntegration(),
            ],
        )
        sentry_sdk.utils.MAX_STRING_LENGTH = 2048

    ddtrace.config.version = VERSION
    statsd.constant_tags.append(f"service:{SERVICE_NAME}")
    ddtrace.config.service = SERVICE_NAME

    ddtrace.config.httpx["split_by_domain"] = True
    ddtrace.tracer.on_start_span(ddtrace_hook)

    logs.setup_logging(dump_config=dump_config)

    models.init_sqlalchemy(pool_size=pg_pool_size)

    # NOTE(sileht): For security reason, we don't expose env after this point
    # env is authorized during modules loading and pre service initializarion
    # after it's not.
    envs_to_preserve = ("PATH", "LANG", "VIRTUAL_ENV")

    saved_env = {
        env: os.environ[env]
        for env in os.environ
        if env in envs_to_preserve or env.startswith("DD_")
    }
    os.environ.clear()
    os.environ.update(saved_env)
