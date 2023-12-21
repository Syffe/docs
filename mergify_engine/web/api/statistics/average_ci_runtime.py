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
from mergify_engine.web.api.statistics import utils


router = fastapi.APIRouter()


class RuntimeInInterval(typing_extensions.TypedDict):
    start: datetime.datetime
    end: datetime.datetime
    runtime: float


class CIRuntimeGroupResponse(typing_extensions.TypedDict):
    base_ref: github_types.GitHubRefType
    partition_name: partr_config.PartitionRuleName
    queue_name: queue_rules.QueueName
    stats: list[RuntimeInInterval]


class CIAverageRuntimeResponse(typing_extensions.TypedDict):
    groups: list[CIRuntimeGroupResponse]


def format_response(result: list[dict[str, typing.Any]]) -> CIAverageRuntimeResponse:
    group_func = operator.itemgetter("base_ref", "partition_name", "queue_name")

    return CIAverageRuntimeResponse(
        groups=[
            CIRuntimeGroupResponse(
                base_ref=group_key[0],
                partition_name=group_key[1],
                queue_name=group_key[2],
                stats=[
                    RuntimeInInterval(
                        start=interval["start"],
                        end=interval["end"],
                        runtime=interval["runtime"],
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
    "/repos/{owner}/{repository}/stats/average_ci_runtime",
    summary="Get the average CI runtime",
    description="Runtime of your CI running on queued pull requests by intervals of time",
    responses=api.default_responses,
)
async def get_average_ci_runtime(
    session: database.Session,
    repository: security.Repository,
    start_end: utils.StatsStartEnd,
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
) -> CIAverageRuntimeResponse:
    start_at = typing.cast(datetime.datetime, start_end[0])
    end_at = typing.cast(datetime.datetime, start_end[1])

    result = (
        await evt_models.EventActionQueueChecksEnd.get_average_ci_runtime_by_interval(
            session,
            repository,
            start_at,
            end_at,
            utils.get_interval(end_at - start_at),
            base_ref,
            partition_name,
            queue_name,
        )
    )

    return format_response([r._asdict() for r in result])
