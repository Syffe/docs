import daiquiri
import msgpack
import pydantic_core

from mergify_engine import database
from mergify_engine import exceptions
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.models import github as gh_models


LOG = daiquiri.getLogger(__name__)


HandledModelsT = type[gh_models.PullRequest] | type[gh_models.CheckRun]

EVENT_TO_MODEL_MAPPING: dict[str, HandledModelsT] = {
    "pull_request": gh_models.PullRequest,
    "check_run": gh_models.CheckRun,
}


async def store_redis_events_in_pg(redis_links: redis_utils.RedisLinks) -> None:
    async for event_id, event in redis_utils.iter_stream(
        redis_links.stream,
        "github_in_postgres",
        settings.GITHUB_IN_POSTGRES_PROCESSING_BATCH_SIZE,
    ):
        event_type = event[b"event_type"].decode()
        LOG.info("processing event '%s', id=%s", event_type, event_id)

        if event_type not in EVENT_TO_MODEL_MAPPING:
            LOG.error(
                "Found unhandled event_type '%s' in 'github_in_postgres' stream",
                event_type,
                event=event,
                event_id=event_id,
            )
            continue

        event_data = msgpack.unpackb(event[b"data"])
        model = EVENT_TO_MODEL_MAPPING[event_type]
        try:
            typed_event_data = model.type_adapter.validate_python(event_data)
        except pydantic_core.ValidationError:
            LOG.warning(
                "Dropping event %s/id=%s because it cannot be validated by its model's type adapter",
                event_type,
                event_id,
                raw_event=event,
                event_data=event_data,
            )
            await redis_links.stream.xdel("github_in_postgres", event_id)
            continue

        async with database.create_session() as session:
            try:
                # mypy thinks the typed_event_data can be, for example,
                # `CheckRun` for a "pull_request" event.
                await model.insert_or_update(session, typed_event_data)  # type: ignore[arg-type]
            except exceptions.MergifyNotInstalled:
                # Just ignore event for uninstalled repository
                pass
            except Exception as e:
                if exceptions.should_be_ignored(e):
                    pass
                elif exceptions.need_retry_in(e):
                    # TODO(sileht): We should retry later, not on next iteration
                    LOG.warning(
                        "Event need to be retried",
                        event_type=event_type,
                        event_id=event_id,
                        raw_event=event,
                        event_data=event_data,
                        exc_info=True,
                    )
                    continue
                else:
                    # TODO(sileht): We should have a better retry mechanism
                    LOG.error(
                        "Event can't be processed",
                        event_type=event_type,
                        event_id=event_id,
                        raw_event=event,
                        event_data=event_data,
                        exc_info=True,
                    )
                    continue
            else:
                await session.commit()

        await redis_links.stream.xdel("github_in_postgres", event_id)
