import typing

import daiquiri
import fastapi
import pydantic

from mergify_engine import database
from mergify_engine import eventlogs
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine.web import api
from mergify_engine.web.api import security


LOG = daiquiri.getLogger(__name__)


router = fastapi.APIRouter(
    tags=["eventlogs"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


Event = typing.Annotated[
    eventlogs.Event,
    pydantic.Field(discriminator="event"),
]


class EventLogsResponse(pagination.PageResponse[Event]):
    items_key: typing.ClassVar[str] = "events"
    events: list[Event] = pydantic.Field(
        json_schema_extra={
            "metadata": {
                "description": "The list of events of a pull request",
            },
        }
    )


content_example = {
    "application/json": {
        "example": {
            "size": 0,
            "per_page": 0,
            "total": 0,
            "events": [
                {
                    "id": 0,
                    "timestamp": "2019-08-24T14:15:22Z",
                    "received_at": "2019-08-24T14:15:22Z",
                    "trigger": "string",
                    "repository": "string",
                    "pull_request": 0,
                    "event": "action.assign",
                    "type": "action.assign",
                    "metadata": {
                        "added": ["string"],
                        "removed": ["string"],
                    },
                }
            ],
        }
    }
}


@router.get(
    "/repos/{owner}/{repository}/pulls/{pull}/events",
    summary="Get the events log of a pull request",
    description="Get the events log of the requested pull request",
    deprecated=True,
    dependencies=[fastapi.Depends(security.check_subscription_feature_eventlogs)],
    response_model=EventLogsResponse,
    responses={
        **api.default_responses,  # type: ignore
        200: {
            "headers": pagination.LinkHeader,
            "content": content_example,
        },
    },
)
async def get_pull_request_eventlogs(
    session: database.Session,
    repository_ctxt: security.Repository,
    pull: typing.Annotated[
        github_types.GitHubPullRequestNumber,
        fastapi.Path(description="Pull request number"),
    ],
    current_page: pagination.CurrentPage,
) -> EventLogsResponse:
    # avoid circular import
    from mergify_engine import events

    if not await eventlogs.use_events_redis_backend(repository_ctxt):
        # NOTE(lecrepont01): ensure transition from redis db to postgreSQL
        page = await events.get(
            session, current_page, repository_ctxt, pull, old_format=True
        )
    else:
        page = await eventlogs.get(repository_ctxt, current_page, pull)

    return EventLogsResponse(page)  # type: ignore[misc, call-arg]


@router.get(
    "/repos/{owner}/{repository}/events",
    summary="Get the events log of a repository",
    description="Get the events log of the requested repository",
    deprecated=True,
    dependencies=[fastapi.Depends(security.check_subscription_feature_eventlogs)],
    response_model=EventLogsResponse,
    responses={
        **api.default_responses,  # type: ignore
        200: {
            "headers": pagination.LinkHeader,
            "content": content_example,
        },
    },
)
async def get_repository_eventlogs(
    session: database.Session,
    repository_ctxt: security.Repository,
    current_page: pagination.CurrentPage,
) -> EventLogsResponse:
    # avoid circular import
    from mergify_engine import events

    if not await eventlogs.use_events_redis_backend(repository_ctxt):
        page = await events.get(session, current_page, repository_ctxt, old_format=True)
    else:
        page = await eventlogs.get(repository_ctxt, current_page)

    return EventLogsResponse(page)  # type: ignore[misc, call-arg]
