import contextvars
import logging
import os
import sys
from urllib import parse

import daiquiri
import daiquiri.formatter
import ddtrace

from mergify_engine import config
from mergify_engine import settings


LOG = daiquiri.getLogger(__name__)


logging.addLevelName(42, "TEST")
LEVEL_COLORS = daiquiri.formatter.ColorFormatter.LEVEL_COLORS.copy()
LEVEL_COLORS[42] = "\033[01;35m"


WORKER_ID: contextvars.ContextVar[str] = contextvars.ContextVar("worker_id")


class CustomFormatter(daiquiri.formatter.ColorExtrasFormatter):
    LEVEL_COLORS = LEVEL_COLORS

    def format(self, record: logging.LogRecord) -> str:
        if hasattr(record, "_daiquiri_extra_keys"):
            record._daiquiri_extra_keys = sorted(record._daiquiri_extra_keys)
        return super().format(record)

    def add_extras(self, record: daiquiri.types.ExtrasLogRecord) -> None:
        super().add_extras(record)
        worker_id = WORKER_ID.get(None)
        if worker_id is not None:
            record.extras += " " + self.extras_template.format("worker_id", worker_id)


CUSTOM_FORMATTER = CustomFormatter(
    fmt="%(asctime)s [%(process)d] %(color)s%(levelname)-8.8s %(name)s: \033[1m%(message)s\033[0m%(extras)s%(color_stop)s"
)


class HerokuDatadogFormatter(daiquiri.formatter.DatadogFormatter):
    # NOTE(sileht): for security reason we empty the os.environ at runtime
    # We can access it only when modules load.
    HEROKU_LOG_EXTRAS = {
        envvar: os.environ[envvar]
        for envvar in ("HEROKU_RELEASE_VERSION",)
        if envvar in os.environ
    }

    def add_fields(
        self,
        log_record: dict[str, str],
        record: logging.LogRecord,
        message_dict: dict[str, str],
    ) -> None:
        super().add_fields(log_record, record, message_dict)
        log_record.update(self.HEROKU_LOG_EXTRAS)
        log_record.update(
            {
                f"dd.{k}": v
                for k, v in ddtrace.tracer.get_log_correlation_context().items()
            }
        )
        worker_id = WORKER_ID.get(None)
        if worker_id is not None:
            log_record.update({"worker_id": worker_id})


def strip_url_credentials(url: str) -> str:
    parsed = parse.urlparse(url)
    if parsed.password or parsed.username:
        netloc = "*****@"
        if parsed.hostname is not None:
            netloc += parsed.hostname
        if parsed.port is not None:
            netloc += f":{parsed.port}"
        url = parsed._replace(netloc=netloc).geturl()
    return url


def config_log() -> None:
    LOG.info("##################### CONFIGURATION ######################")
    for key, value in settings.dict().items():
        if key.startswith("TESTING_"):
            continue
        LOG.info("* %s: %s", key, value)

    for key, value in config.CONFIG.items():
        name = str(key)
        if (
            name == "OAUTH_CLIENT_ID"
            or "TOKEN" in name
            or "SECRET" in name
            or "KEY" in name
        ) and value is not None:
            value = "*****"
        if "URL" in name and value is not None:
            if isinstance(value, list):
                value = [strip_url_credentials(v) for v in value]
            else:
                value = strip_url_credentials(value)
        LOG.info("* MERGIFYENGINE_%s: %s", name, value)
    LOG.info("* PATH: %s", os.environ.get("PATH"))
    LOG.info("##########################################################")

    if os.getenv("MERGIFYENGINE_STORAGE_URL") is not None:
        LOG.warning(
            "MERGIFYENGINE_STORAGE_URL is set, on-premise legacy Redis database setup detected."
        )

    for env in (
        "GITHUB_API_URL",
        "GITHUB_REST_API_URL",
        "GITHUB_GRAPHQL_API_URL",
        "BOT_USER_ID",
        "BOT_USER_LOGIN",
    ):
        if f"MERGIFYENGINE_{env}" in os.environ:
            LOG.warning(
                f"MERGIFYENGINE_{env} configuration environment variable is deprecated and can be removed"
            )


def setup_logging(dump_config: bool = True) -> None:
    outputs: list[daiquiri.output.Output] = []

    if settings.LOG_STDOUT:
        outputs.append(
            daiquiri.output.Stream(
                sys.stdout, level=settings.LOG_STDOUT_LEVEL, formatter=CUSTOM_FORMATTER
            )
        )

    if settings.LOG_DATADOG:
        dd_extras: dict[str, int | str] = {}
        if isinstance(settings.LOG_DATADOG, str):
            dd_agent_parsed = parse.urlparse(settings.LOG_DATADOG)
            if dd_agent_parsed.scheme != "udp":
                raise RuntimeError(
                    "Only UDP protocol is supported for MERGIFYENGINE_LOG_DATADOG"
                )
            if dd_agent_parsed.hostname:
                dd_extras["hostname"] = dd_agent_parsed.hostname
            if dd_agent_parsed.port:
                dd_extras["port"] = dd_agent_parsed.port
        outputs.append(
            daiquiri.output.Datadog(
                level=settings.LOG_DATADOG_LEVEL,
                handler_class=daiquiri.handlers.PlainTextDatagramHandler,
                formatter=HerokuDatadogFormatter(),
                **dd_extras,  # type:ignore [arg-type]
            )
        )

    daiquiri.setup(
        outputs=outputs,
        level=settings.LOG_LEVEL,
    )
    daiquiri.set_default_log_levels(
        [
            ("github.Requester", "WARN"),
            ("urllib3.connectionpool", "WARN"),
            ("urllib3.util.retry", "WARN"),
            ("vcr", "WARN"),
            ("httpx", "WARN"),
            ("asyncio", "WARN"),
            ("ddtrace", "WARN"),
            ("uvicorn.access", "WARN"),
        ]
        + [(name, "DEBUG") for name in settings.LOG_DEBUG_LOGGER_NAMES]
    )

    if dump_config:
        config_log()
