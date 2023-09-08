import dataclasses
import typing

import daiquiri
import fastapi
import pydantic

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


@pydantic.dataclasses.dataclass
class EventLogsResponse(pagination.PageResponse[Event]):
    items_key = "events"
    events: list[Event] = dataclasses.field(
        init=False,
        metadata={
            "description": "The list of events of a pull request",
        },
    )


@router.get(
    "/repos/{owner}/{repository}/pulls/{pull}/events",
    summary="Get the events log of a pull request",
    description="Get the events log of the requested pull request",
    dependencies=[fastapi.Depends(security.check_subscription_feature_eventlogs)],
    response_model=EventLogsResponse,
    responses={
        **api.default_responses,  # type: ignore
        200: {
            "headers": pagination.LinkHeader,
        },
    },
)
async def get_pull_request_eventlogs(
    repository_ctxt: security.Repository,
    pull: typing.Annotated[
        github_types.GitHubPullRequestNumber,
        fastapi.Path(description="Pull request number"),
    ],
    current_page: pagination.CurrentPage,
) -> EventLogsResponse:
    page = await eventlogs.get(repository_ctxt, current_page, pull)
    return EventLogsResponse(page)


@router.get(
    "/repos/{owner}/{repository}/events",
    summary="Get the events log of a repository",
    description="Get the events log of the requested repository",
    dependencies=[fastapi.Depends(security.check_subscription_feature_eventlogs)],
    response_model=EventLogsResponse,
    responses={
        **api.default_responses,  # type: ignore
        200: {
            "headers": pagination.LinkHeader,
        },
    },
)
async def get_repository_eventlogs(
    repository_ctxt: security.Repository,
    current_page: pagination.CurrentPage,
) -> EventLogsResponse:
    page = await eventlogs.get(repository_ctxt, current_page)
    return EventLogsResponse(page)
