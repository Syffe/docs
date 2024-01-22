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


class MergedInInterval(typing_extensions.TypedDict):
    start: datetime.datetime
    end: datetime.datetime
    merged: int


class MergedGroupResponse(typing_extensions.TypedDict):
    base_ref: github_types.GitHubRefType
    partition_names: list[partr_config.PartitionRuleName]
    queue_name: queue_rules.QueueName
    stats: list[MergedInInterval]


class MergedCountResponse(typing_extensions.TypedDict):
    groups: list[MergedGroupResponse]


def format_response(result: list[dict[str, typing.Any]]) -> MergedCountResponse:
    group_func = operator.itemgetter("base_ref", "partition_names", "queue_name")

    return MergedCountResponse(
        groups=[
            MergedGroupResponse(
                base_ref=group_key[0],
                partition_names=group_key[1],
                queue_name=group_key[2],
                stats=[
                    MergedInInterval(
                        start=interval["start"],
                        end=interval["end"],
                        merged=interval["merged"],
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
    "/repos/{owner}/{repository}/stats/queues_merged_count",
    summary="Get the count of pull requests merged by queues",
    description="Queues pull requests merged by intervals of time",
    responses=api.default_responses,
)
async def get_queues_pull_requests_merged_count(
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
) -> MergedCountResponse:
    result = await evt_models.EventActionQueueMerged.get_merged_count_by_interval(
        session,
        repository,
        timerange,
        base_ref,
        partition_name,
        queue_name,
    )

    return format_response([r._asdict() for r in result])
