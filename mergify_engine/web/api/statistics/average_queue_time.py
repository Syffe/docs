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
from mergify_engine.web.api import security


router = fastapi.APIRouter()


class QueueTimeInInterval(typing_extensions.TypedDict):
    start: datetime.datetime
    end: datetime.datetime
    queue_time: float


class QueueTimeGroupResponse(typing_extensions.TypedDict):
    base_ref: github_types.GitHubRefType
    partition_name: partr_config.PartitionRuleName
    queue_name: queue_rules.QueueName
    stats: list[QueueTimeInInterval]


class QueueAverageTimeResponse(typing_extensions.TypedDict):
    groups: list[QueueTimeGroupResponse]


def format_response(result: list[dict[str, typing.Any]]) -> QueueAverageTimeResponse:
    group_func = operator.itemgetter("base_ref", "partition_name", "queue_name")

    return QueueAverageTimeResponse(
        groups=[
            QueueTimeGroupResponse(
                base_ref=group_key[0],
                partition_name=group_key[1],
                queue_name=group_key[2],
                stats=[
                    QueueTimeInInterval(
                        start=interval["start"],
                        end=interval["end"],
                        queue_time=interval["queued_idle_time"],
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
    "/repos/{owner}/{repository}/stats/average_queue_time",
    summary="Get the average idle queue time",
    description="Idle time spent in queue by the Pull Requests by intervals of time",
    responses=api.default_responses,
)
async def get_average_idle_queue_time(
    session: database.Session,
    repository: security.Repository,
    timerange: security.TimeRange,
    base_ref: typing.Annotated[
        list[github_types.GitHubRefType] | None,
        fastapi.Query(description="Base reference(s) of the pull requests"),
    ] = None,
    partition_name: typing.Annotated[
        list[partr_config.PartitionRuleName] | None,
        fastapi.Query(description="Partition name(s) of the pull requests"),
    ] = None,
    queue_name: typing.Annotated[
        list[queue_rules.QueueName] | None,
        fastapi.Query(description="Name of the merge queue(s) for the pull requests"),
    ] = None,
) -> QueueAverageTimeResponse:
    result = (
        await evt_models.EventActionQueueLeave.get_average_idle_queue_time_by_interval(
            session,
            repository,
            timerange,
            base_ref,
            partition_name,
            queue_name,
        )
    )

    return format_response([r._asdict() for r in result])
