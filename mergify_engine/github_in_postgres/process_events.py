import dataclasses

import daiquiri
from ddtrace import tracer
import msgpack
import pydantic_core

from mergify_engine import database
from mergify_engine import exceptions
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.models import github as gh_models


LOG = daiquiri.getLogger(__name__)


HandledModelsT = (
    type[gh_models.PullRequest] | type[gh_models.CheckRun] | type[gh_models.Status]
)

EVENT_TO_MODEL_MAPPING: dict[str, HandledModelsT] = {
    "pull_request": gh_models.PullRequest,
    "check_run": gh_models.CheckRun,
    "status": gh_models.Status,
}


@dataclasses.dataclass
class UnhandledEventTypeError(Exception):
    message: str
    event_type: str


async def store_redis_events_in_pg(redis_links: redis_utils.RedisLinks) -> bool:
    return await redis_utils.process_stream(
        "github-in-postgres",
        redis_links.stream,
        redis_key="github_in_postgres",
        batch_size=settings.GITHUB_IN_POSTGRES_PROCESSING_BATCH_SIZE,
        event_processor=store_redis_event_in_pg,
    )


@tracer.wrap("store_redis_event_in_pg")
async def store_redis_event_in_pg(event_id: bytes, event: dict[bytes, bytes]) -> None:
    event_type = event[b"event_type"].decode()
    LOG.info("processing event '%s', id=%s", event_type, event_id)

    if event_type not in EVENT_TO_MODEL_MAPPING:
        raise UnhandledEventTypeError(
            "Found unhandled event_type '%s' in 'github_in_postgres' stream"
            % event_type,
            event_type,
        )

    event_data = msgpack.unpackb(event[b"data"])
    model = EVENT_TO_MODEL_MAPPING[event_type]
    try:
        typed_event_data = model.type_adapter.validate_python(event_data)
    except pydantic_core.ValidationError:
        LOG.warning(
            "Dropping event because it cannot be validated by its model's type adapter",
            event_type=event_type,
            event_id=event_id,
            # FIXME(sileht): we need to find another way to get the event data
            # that make the processing failing.
            # raw_event=event,
            # event_data=event_data,
            exc_info=True,
        )
        return

    async with database.create_session() as session:
        try:
            # mypy thinks the typed_event_data can be, for example,
            # `CheckRun` for a "pull_request" event.
            await model.insert_or_update(session, typed_event_data)  # type: ignore[arg-type]
        except exceptions.MergifyNotInstalledError:
            # Just ignore event for uninstalled repository
            return
        await session.commit()
