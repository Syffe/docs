import datetime
import itertools
import operator
import typing

import fastapi
from sqlalchemy import func
from sqlalchemy.dialects import postgresql
import sqlalchemy.sql.functions
import typing_extensions

from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine.models import events as evt_models
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules
from mergify_engine.web import api
from mergify_engine.web.api import security
from mergify_engine.web.api.statistics import utils


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


def get_interval(
    duration: datetime.timedelta,
) -> tuple[sqlalchemy.sql.functions.Function[postgresql.INTERVAL], ...]:
    # aggregation interval depending on the wanted duration = end_at - start_at
    if duration <= datetime.timedelta(days=2):
        # 1 value per hour (max 48 points)
        return func.make_interval(0, 0, 0, 0, 1), func.make_interval(
            0,
            0,
            0,
            0,
            0,
            59,
            59,
        )
    if duration <= datetime.timedelta(days=7):
        # 1 value per 4 hours (max 42 points)
        return func.make_interval(0, 0, 0, 0, 4), func.make_interval(
            0,
            0,
            0,
            0,
            3,
            59,
            59,
        )
    if duration <= datetime.timedelta(days=60):
        # 1 value per day (max 60 points)
        return func.make_interval(0, 0, 0, 1), func.make_interval(
            0,
            0,
            0,
            0,
            23,
            59,
            59,
        )
    # 1 value per 2 days (max 45 points)
    return func.make_interval(0, 0, 0, 2), func.make_interval(
        0,
        0,
        0,
        1,
        23,
        59,
        59,
    )


@router.get(
    "/repos/{owner}/{repository}/stats/queues_merged_count",
    summary="Get the count of pull requests merged by queues",
    description="Queues pull requests merged by intervals of time",
    responses={
        **api.default_responses,  # type: ignore[dict-item]
    },
)
async def get_queues_pull_requests_merged_count(
    session: database.Session,
    repository: security.Repository,
    start_at: typing.Annotated[
        utils.DatetimeNotInFuture | None,
        fastapi.Query(description="Get the stats from this date"),
    ] = None,
    end_at: typing.Annotated[
        utils.DatetimeNotInFuture | None,
        fastapi.Query(description="Get the stats until this date"),
    ] = None,
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
) -> MergedCountResponse:
    end_at_ = date.utcnow() if end_at is None else end_at
    start_at_ = end_at_ - datetime.timedelta(days=1) if start_at is None else start_at

    if end_at_ < start_at_:
        raise fastapi.HTTPException(
            status_code=422,
            detail="provided end_at should be after start_at",
        )

    result = await evt_models.EventActionQueueMerged.get_merged_count_by_interval(
        session,
        repository,
        start_at_,
        end_at_,
        get_interval(end_at_ - start_at_),
        base_ref,
        partition_name,
        queue_name,
    )

    return format_response([r._asdict() for r in result])
