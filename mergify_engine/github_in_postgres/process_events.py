import typing

import daiquiri
import msgpack

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.models import github as gh_models


LOG = daiquiri.getLogger(__name__)


EVENT_TO_MODEL_MAPPING = {
    "pull_request": gh_models.PullRequest,
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
        if event_type == "pull_request":
            typed_event_data = typing.cast(github_types.GitHubPullRequest, event_data)
        else:
            raise RuntimeError(
                f"Should not have landed here with event_type {event_type}"
            )

        async with database.create_session() as session:
            await EVENT_TO_MODEL_MAPPING[event_type].insert_or_update(
                session, typed_event_data
            )
            await session.commit()

        await redis_links.stream.xdel("github_in_postgres", event_id)
