import os
import typing

from datadog import statsd  # type: ignore[attr-defined]
import ddtrace
import sentry_sdk
from sentry_sdk.integrations import fastapi
from sentry_sdk.integrations import httpx
from sentry_sdk.integrations import sqlalchemy
from sentry_sdk.integrations import starlette

from mergify_engine import database
from mergify_engine import logs
from mergify_engine import settings


SERVICE_NAME: str = "engine-<unknown>"
VERSION: str
if settings.SAAS_MODE:
    VERSION = settings.SHA
else:
    VERSION = settings.VERSION


def ddtrace_hook(span: ddtrace.Span) -> None:
    root_span = ddtrace.tracer.current_root_span()
    if root_span is None:
        return

    owner = root_span.get_tag("gh_owner")
    if owner and span.get_tag("gh_owner") is None:
        span.set_tag("gh_owner", owner)


def sentry_before_send(
    event: dict[str, typing.Any],
    hint: dict[str, typing.Any],
) -> dict[str, typing.Any] | None:
    # NOTE(sileht): malicious user can craft a traceparent header with an invalid version, this is logged as an exception
    # by ddtrace
    for message in (
        "received invalid w3c traceparent:",
        "Caught handled exception, but response already started.",
    ):
        if message in event.get("message", ""):
            return None
    return event


def setup(
    service_name: str,
    dump_config: bool = True,
    stdout_logging_only: bool = False,
) -> None:
    global SERVICE_NAME
    SERVICE_NAME = "engine-" + service_name

    if settings.SENTRY_URL and not stdout_logging_only:  # pragma: no cover
        sentry_sdk.init(
            settings.SENTRY_URL.geturl(),
            max_breadcrumbs=10,
            release=VERSION,
            environment=settings.SENTRY_ENVIRONMENT,
            max_request_body_size="never",
            max_value_length=2048,
            integrations=[
                httpx.HttpxIntegration(),
                starlette.StarletteIntegration(),
                fastapi.FastApiIntegration(),
                sqlalchemy.SqlalchemyIntegration(),
            ],
            before_send=sentry_before_send,
        )

    ddtrace.config.version = VERSION
    statsd.constant_tags.append(f"service:{SERVICE_NAME}")
    ddtrace.config.service = SERVICE_NAME

    ddtrace.config.httpx["split_by_domain"] = True
    ddtrace.tracer.on_start_span(ddtrace_hook)

    logs.setup_logging(dump_config=dump_config, stdout_logging_only=stdout_logging_only)

    database.init_sqlalchemy(service_name)

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
