import datetime
import itertools
import operator
import typing

import fastapi
import typing_extensions

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine.models import events as evt_models
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules
from mergify_engine.web import api
from mergify_engine.web import utils
from mergify_engine.web.api import security


router = fastapi.APIRouter()


class QueueSizeInInterval(typing_extensions.TypedDict):
    start: datetime.datetime
    end: datetime.datetime
    avg_size: float
    max_size: int
    min_size: int


class QueueSizeGroupResponse(typing_extensions.TypedDict):
    base_ref: github_types.GitHubRefType
    partition_name: partr_config.PartitionRuleName
    queue_name: queue_rules.QueueName
    stats: list[QueueSizeInInterval]


class QueueSizeResponse(typing_extensions.TypedDict):
    groups: list[QueueSizeGroupResponse]


def format_response(result: list[dict[str, typing.Any]]) -> QueueSizeResponse:
    group_func = operator.itemgetter("base_ref", "partition_name", "queue_name")

    return QueueSizeResponse(
        groups=[
            QueueSizeGroupResponse(
                base_ref=group_key[0],
                partition_name=group_key[1],
                queue_name=group_key[2],
                stats=[
                    QueueSizeInInterval(
                        start=interval["start"],
                        end=interval["end"],
                        avg_size=interval["avg_size"],
                        max_size=interval["max_size"],
                        min_size=interval["min_size"],
                    )
                    for interval in group
                ],
            )
            for group_key, group in itertools.groupby(
                result,
                key=group_func,
            )
        ],
    )


@router.get(
    "/repos/{owner}/{repository}/stats/queue_size",
    summary="Get the queue size",
    description="Get the average, max and min queue size over the period by intervals of time",
    responses=api.default_responses,
)
async def get_queue_size(
    session: database.Session,
    repository: security.Repository,
    timerange: security.TimeRange,
    base_ref: typing.Annotated[
        list[utils.PostgresTextField[github_types.GitHubRefType]] | None,
        fastapi.Query(description="Base reference(s) of the pull requests"),
    ] = None,
    partition_name: typing.Annotated[
        list[utils.PostgresTextField[partr_config.PartitionRuleName]] | None,
        fastapi.Query(description="Partition name(s) of the pull requests"),
    ] = None,
    queue_name: typing.Annotated[
        list[utils.PostgresTextField[queue_rules.QueueName]] | None,
        fastapi.Query(description="Name of the merge queue(s) for the pull requests"),
    ] = None,
) -> QueueSizeResponse:
    result = await evt_models.EventActionQueueChange.get_queue_size_by_interval(
        session,
        repository,
        timerange,
        base_ref,
        partition_name,
        queue_name,
    )

    return format_response([r._asdict() for r in result])
