from __future__ import annotations

import dataclasses
import datetime
import typing

import daiquiri
import fastapi
import pydantic
from starlette.status import HTTP_204_NO_CONTENT

from mergify_engine import constants
from mergify_engine import context
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine.dashboard import application as application_mod
from mergify_engine.queue import freeze
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules import conditions as rules_conditions
from mergify_engine.web import api
from mergify_engine.web.api import security
from mergify_engine.web.api import statistics as statistics_api


LOG = daiquiri.getLogger(__name__)

router = fastapi.APIRouter(
    tags=["queues"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


@pydantic.dataclasses.dataclass
class Branch:
    name: github_types.GitHubRefType = dataclasses.field(
        metadata={"description": "The name of the branch"}
    )


@pydantic.dataclasses.dataclass
class SpeculativeCheckPullRequest:
    in_place: bool = dataclasses.field(
        metadata={"description": "Whether the pull request has been checked in-place"}
    )
    number: github_types.GitHubPullRequestNumber = dataclasses.field(
        metadata={
            "description": "The number of the pull request used by the speculative check"
        }
    )
    started_at: datetime.datetime | None = dataclasses.field(
        metadata={
            "description": "The timestamp when the checks have started for this pull request"
        }
    )
    ended_at: datetime.datetime | None = dataclasses.field(
        metadata={
            "description": "The timestamp when the checks have ended for this pull request"
        }
    )
    checks: list[merge_train.QueueCheck] = dataclasses.field(
        metadata={"description": "The list of pull request checks"}
    )
    evaluated_conditions: str = dataclasses.field(
        metadata={"description": "The queue rule conditions evaluation report"}
    )
    state: merge_train.CheckStateT = dataclasses.field(
        metadata={"description": "The global state of the checks"}
    )

    @classmethod
    def from_train_car(
        cls,
        car: merge_train.TrainCar | None,
    ) -> SpeculativeCheckPullRequest | None:
        if car is None:
            return None
        elif car.train_car_state.checks_type in (
            merge_train.TrainCarChecksType.FAILED,
            None,
        ):
            return None
        elif car.train_car_state.checks_type in (
            merge_train.TrainCarChecksType.DRAFT,
            merge_train.TrainCarChecksType.INPLACE,
        ):
            if car.queue_pull_request_number is None:
                raise RuntimeError(
                    f"car's checks type is {car.train_car_state.checks_type}, but queue_pull_request_number is None"
                )
            return cls(
                in_place=car.train_car_state.checks_type
                == merge_train.TrainCarChecksType.INPLACE,
                number=car.queue_pull_request_number,
                started_at=car.train_car_state.ci_started_at,
                ended_at=car.checks_ended_timestamp,
                state=car.get_queue_check_run_conclusion().value or "pending",
                checks=car.last_checks,
                evaluated_conditions=car.last_evaluated_conditions,
            )
        else:
            raise RuntimeError(
                f"Car's checks type unknown: {car.train_car_state.checks_type}"
            )


@pydantic.dataclasses.dataclass
class BriefMergeabilityCheck:
    check_type: typing.Literal["in_place", "draft_pr"] = dataclasses.field(
        metadata={"description": "The type of queue check (in_place or draft_pr)"}
    )
    pull_request_number: github_types.GitHubPullRequestNumber = dataclasses.field(
        metadata={
            "description": "The number of the pull request used by the speculative check"
        }
    )
    started_at: datetime.datetime = dataclasses.field(
        metadata={
            "description": "The timestamp when the checks have started for this pull request"
        }
    )
    ended_at: datetime.datetime | None = dataclasses.field(
        metadata={
            "description": "The timestamp when the checks have ended for this pull request"
        }
    )
    state: merge_train.CheckStateT = dataclasses.field(
        metadata={"description": "The global state of the checks"}
    )

    @classmethod
    def from_train_car(
        cls, car: merge_train.TrainCar | None
    ) -> BriefMergeabilityCheck | None:
        mergeability_check = MergeabilityCheck.from_train_car(car)
        if mergeability_check is not None:
            return cls(**dataclasses.asdict(mergeability_check))
        return None


@pydantic.dataclasses.dataclass
class QueueRule:
    name: rules.QueueName = dataclasses.field(
        metadata={"description": "The name of the queue rule"}
    )

    config: rules.QueueConfig = dataclasses.field(
        metadata={"description": "The configuration of the queue rule"}
    )


@pydantic.dataclasses.dataclass
class PullRequestQueued:
    number: github_types.GitHubPullRequestNumber = pydantic.Field(
        description="The number of the pull request"
    )
    position: int = pydantic.Field(
        description="The position of the pull request in the queue. The first pull request in the queue is at position 0"
    )
    priority: int = pydantic.Field(description="The priority of this pull request")
    queue_rule: QueueRule = pydantic.Field(
        description="The queue rule associated to this pull request"
    )
    queued_at: datetime.datetime = pydantic.Field(
        description="The timestamp when the pull requested has entered in the queue"
    )
    speculative_check_pull_request: SpeculativeCheckPullRequest | None = pydantic.Field(
        ..., deprecated=True, description="Use `mergeability_check` instead"
    )
    mergeability_check: BriefMergeabilityCheck | None = pydantic.Field(
        description="Information about the mergeability check currently processed"
    )
    estimated_time_of_merge: datetime.datetime | None = pydantic.Field(
        description="The estimated timestamp when this pull request will be merged"
    )


@pydantic.dataclasses.dataclass
class Queue:
    branch: Branch = dataclasses.field(
        metadata={"description": "The branch of this queue"}
    )

    pull_requests: list[PullRequestQueued] = dataclasses.field(
        default_factory=list,
        metadata={"description": "The pull requests in this queue"},
    )


@pydantic.dataclasses.dataclass
class QueuesConfig:
    configuration: list[QueueRule] = dataclasses.field(
        default_factory=list,
        metadata={"description": "The queues configuration of the repository"},
    )


@pydantic.dataclasses.dataclass
class Queues:
    queues: list[Queue] = dataclasses.field(
        default_factory=list, metadata={"description": "The queues of the repository"}
    )


# FIXME(sileht): reuse dataclasses variante once
# https://github.com/tiangolo/fastapi/issues/4679 is fixed
class QueueFreezePayload(pydantic.BaseModel):
    reason: str = pydantic.Field(
        max_length=255, description="The reason of the queue freeze"
    )
    cascading: bool = pydantic.Field(
        default=True, description="The active status of the cascading effect"
    )


@pydantic.dataclasses.dataclass
class QueueFreeze:
    application_name: str = dataclasses.field(
        metadata={"description": "Application name responsible for the freeze"},
    )
    application_id: int = dataclasses.field(
        metadata={"description": "Application ID responsible for the freeze"},
    )
    name: str = dataclasses.field(
        default_factory=str, metadata={"description": "Queue name"}
    )
    reason: str = dataclasses.field(
        default_factory=str, metadata={"description": "The reason of the queue freeze"}
    )
    freeze_date: datetime.datetime = dataclasses.field(
        default_factory=date.utcnow,
        metadata={"description": "The date and time of the freeze"},
    )
    cascading: bool = dataclasses.field(
        default=True,
        metadata={"description": "The active status of the cascading effect"},
    )


@pydantic.dataclasses.dataclass
class QueueFreezeResponse:
    queue_freezes: list[QueueFreeze] = dataclasses.field(
        default_factory=list,
        metadata={"description": "The frozen queues of the repository"},
    )


async def get_queue_rules(
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> rules.QueueRules:
    try:
        mergify_config = await repository_ctxt.get_mergify_config()
    except rules.InvalidRules:
        raise fastapi.HTTPException(
            status_code=422,
            detail="The configuration file is invalid.",
        )
    return mergify_config["queue_rules"]


@router.get(
    "/repos/{owner}/{repository}/queues",  # noqa: FS003
    summary="Get merge queues",
    description="Get the list of pull requests queued in a merge queue of a repository",
    response_model=Queues,
    responses={
        **api.default_responses,  # type: ignore
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "queues": [
                            {
                                "branch": {"name": "main"},
                                "pull_requests": [
                                    {
                                        "number": 5678,
                                        "position": 1,
                                        "priority": 100,
                                        "queue_rule": {
                                            "name": "default",
                                            "config": {
                                                "priority": 100,
                                                "batch_size": 1,
                                                "batch_max_wait_time": 0,
                                                "speculative_checks": 2,
                                                "allow_inplace_checks": True,
                                                "allow_queue_branch_edit": False,
                                                "disallow_checks_interruption_from_queues": [],
                                                "checks_timeout": 60,
                                                "draft_bot_account": "",
                                                "queue_branch_prefix": constants.MERGE_QUEUE_BRANCH_PREFIX,
                                                "queue_branch_merge_method": "fast-forward",
                                                "batch_max_failure_resolution_attempts": 10,
                                            },
                                        },
                                        "speculative_check_pull_request": {
                                            "in_place": True,
                                            "number": 5678,
                                            "started_at": "2021-10-14T14:19:12+00:00",
                                            "ended_at": "2021-10-14T15:00:42+00:00",
                                            "checks": [],
                                            "evaluated_conditions": "",
                                            "state": "success",
                                        },
                                        "mergeability_check": {
                                            "check_type": "in_place",
                                            "pull_request_number": 5678,
                                            "started_at": "2021-10-14T14:19:12+00:00",
                                            "ended_at": "2021-10-14T15:00:42+00:00",
                                            "state": "success",
                                        },
                                        "queued_at": "2021-10-14T14:19:12+00:00",
                                        "estimated_time_of_merge": "2021-10-14T15:19:12+00:00",
                                    },
                                    {
                                        "number": 4242,
                                        "position": 1,
                                        "priority": 100,
                                        "queue_rule": {
                                            "name": "default",
                                            "config": {
                                                "priority": 100,
                                                "batch_size": 1,
                                                "batch_max_wait_time": 0,
                                                "speculative_checks": 2,
                                                "allow_inplace_checks": True,
                                                "allow_queue_branch_edit": False,
                                                "disallow_checks_interruption_from_queues": [],
                                                "checks_timeout": 60,
                                                "draft_bot_account": "",
                                                "queue_branch_prefix": constants.MERGE_QUEUE_BRANCH_PREFIX,
                                                "queue_branch_merge_method": "fast-forward",
                                                "batch_max_failure_resolution_attempts": 10,
                                            },
                                        },
                                        "speculative_check_pull_request": {
                                            "in_place": False,
                                            "number": 7899,
                                            "started_at": "2021-10-14T14:19:12+00:00",
                                            "ended_at": "2021-10-14T15:00:42+00:00",
                                            "checks": [],
                                            "evaluated_conditions": "",
                                            "state": "success",
                                        },
                                        "mergeability_check": {
                                            "check_type": "draft_pr",
                                            "pull_request_number": 7899,
                                            "started_at": "2021-10-14T14:19:12+00:00",
                                            "ended_at": "2021-10-14T15:00:42+00:00",
                                            "state": "success",
                                        },
                                        "queued_at": "2021-10-14T14:19:12+00:00",
                                        "estimated_time_of_merge": "2021-10-14T15:19:12+00:00",
                                    },
                                ],
                            }
                        ]
                    }
                }
            }
        },
    },
)
async def repository_queues(
    owner: github_types.GitHubLogin = fastapi.Path(  # noqa: B008
        ..., description="The owner of the repository"
    ),
    repository: github_types.GitHubRepositoryName = fastapi.Path(  # noqa: B008
        ..., description="The name of the repository"
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
    queue_rules: rules.QueueRules = fastapi.Depends(get_queue_rules),  # noqa: B008
) -> Queues:
    queues = Queues()

    checks_duration_stats = (
        await statistics_api.get_checks_duration_stats_for_all_queues(
            repository_ctxt,
        )
    )

    async for train in merge_train.Train.iter_trains(repository_ctxt):
        queue = Queue(Branch(train.ref))
        previous_eta = None
        for position, (embarked_pull, car) in enumerate(train._iter_embarked_pulls()):
            try:
                queue_rule = queue_rules[embarked_pull.config["name"]]
            except KeyError:
                # This car is going to be deleted so skip it
                continue

            speculative_check_pull_request = SpeculativeCheckPullRequest.from_train_car(
                car
            )
            previous_eta = (
                estimated_time_of_merge
            ) = await get_estimated_time_of_merge_from_stats(
                train,
                queue_rules,
                embarked_pull,
                position,
                checks_duration_stats,
                car,
                previous_eta,
            )

            queue.pull_requests.append(
                PullRequestQueued(
                    number=embarked_pull.user_pull_request_number,
                    position=position,
                    priority=embarked_pull.config["priority"],
                    queue_rule=QueueRule(
                        name=embarked_pull.config["name"], config=queue_rule.config
                    ),
                    queued_at=embarked_pull.queued_at,
                    speculative_check_pull_request=speculative_check_pull_request,
                    mergeability_check=BriefMergeabilityCheck.from_train_car(car),
                    estimated_time_of_merge=estimated_time_of_merge,
                )
            )

        queues.queues.append(queue)

    return queues


TRAIN_CAR_CHECKS_TYPE_MAPPING: dict[
    merge_train.TrainCarChecksType, typing.Literal["in_place", "draft_pr"]
] = {
    merge_train.TrainCarChecksType.INPLACE: "in_place",
    merge_train.TrainCarChecksType.DRAFT: "draft_pr",
}


@pydantic.dataclasses.dataclass
class MergeabilityCheck:
    check_type: typing.Literal["in_place", "draft_pr"] = dataclasses.field(
        metadata={"description": "The type of queue check (in_place or draft_pr)"}
    )
    pull_request_number: github_types.GitHubPullRequestNumber = dataclasses.field(
        metadata={
            "description": "The number of the pull request used by the speculative check"
        }
    )
    started_at: datetime.datetime | None = dataclasses.field(
        metadata={
            "description": "The timestamp when the checks have started for this pull request"
        }
    )
    ended_at: datetime.datetime | None = dataclasses.field(
        metadata={
            "description": "The timestamp when the checks have ended for this pull request"
        }
    )
    checks: list[merge_train.QueueCheck] = dataclasses.field(
        metadata={"description": "The list of pull request checks"}
    )
    evaluated_conditions: str = dataclasses.field(
        metadata={"description": "The queue rule conditions evaluation report"}
    )
    state: merge_train.CheckStateT = dataclasses.field(
        metadata={"description": "The global state of the checks"}
    )
    conditions_evaluation: rules_conditions.QueueConditionEvaluationJsonSerialized | None = dataclasses.field(
        metadata={"description": "The queue rule conditions evaluation"}
    )

    @classmethod
    def from_train_car(
        cls,
        car: merge_train.TrainCar | None,
    ) -> MergeabilityCheck | None:
        if car is None:
            return None
        elif car.train_car_state.checks_type in (
            merge_train.TrainCarChecksType.FAILED,
            None,
        ):
            return None
        elif car.train_car_state.checks_type in (
            merge_train.TrainCarChecksType.DRAFT,
            merge_train.TrainCarChecksType.INPLACE,
        ):
            if car.queue_pull_request_number is None:
                raise RuntimeError(
                    f"car's checks type is {car.train_car_state.checks_type}, but queue_pull_request_number is None"
                )
            conditions_evaluation = (
                car.last_conditions_evaluation.as_json_dict()
                if car.last_conditions_evaluation is not None
                else None
            )
            return MergeabilityCheck(
                check_type=TRAIN_CAR_CHECKS_TYPE_MAPPING[
                    car.train_car_state.checks_type
                ],
                pull_request_number=car.queue_pull_request_number,
                started_at=car.train_car_state.ci_started_at,
                ended_at=car.checks_ended_timestamp,
                state=car.get_queue_check_run_conclusion().value or "pending",
                checks=car.last_checks,
                evaluated_conditions=car.last_evaluated_conditions,
                conditions_evaluation=conditions_evaluation,
            )
        else:
            raise RuntimeError(
                f"Car's checks type unknown: {car.train_car_state.checks_type}"
            )


@pydantic.dataclasses.dataclass
class EnhancedPullRequestQueued:
    number: github_types.GitHubPullRequestNumber = dataclasses.field(
        metadata={"description": "The number of the pull request"}
    )

    position: int = dataclasses.field(
        metadata={
            "description": "The position of the pull request in the queue. The first pull request in the queue is at position 0"
        }
    )

    priority: int = dataclasses.field(
        metadata={"description": "The priority of this pull request"}
    )
    queue_rule: QueueRule = dataclasses.field(
        metadata={"description": "The queue rule associated to this pull request"}
    )

    queued_at: datetime.datetime = dataclasses.field(
        metadata={
            "description": "The timestamp when the pull requested has entered in the queue"
        }
    )
    mergeability_check: MergeabilityCheck | None

    estimated_time_of_merge: datetime.datetime | None = dataclasses.field(
        metadata={
            "description": "The estimated timestamp when this pull request will be merged"
        }
    )


@router.get(
    "/repos/{owner}/{repository}/queue/{queue_name}/pull/{pr_number}",  # noqa: FS003
    include_in_schema=False,
    summary="Get a queued pull request",
    description="Get a pull request queued in a merge queue of a repository",
    response_model=EnhancedPullRequestQueued,
    responses={
        **api.default_responses,  # type: ignore
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "number": 5678,
                        "position": 1,
                        "priority": 100,
                        "queue_rule": {
                            "name": "default",
                            "config": {
                                "priority": 100,
                                "batch_size": 1,
                                "batch_max_wait_time": 0,
                                "speculative_checks": 2,
                                "allow_inplace_checks": True,
                                "allow_queue_branch_edit": False,
                                "disallow_checks_interruption_from_queues": [],
                                "checks_timeout": 60,
                                "draft_bot_account": "",
                                "queue_branch_prefix": constants.MERGE_QUEUE_BRANCH_PREFIX,
                                "queue_branch_merge_method": "fast-forward",
                                "batch_max_failure_resolution_attempts": 10,
                            },
                        },
                        "mergeability_check": {
                            "check_type": "draft_pr",
                            "pull_request_number": 5678,
                            "started_at": "2021-10-14T14:19:12+00:00",
                            "ended_at": "2021-10-14T15:00:42+00:00",
                            "checks": [],
                            "evaluated_conditions": "",
                            "conditions_evaluation": {
                                "match": False,
                                "label": "all of",
                                "description": None,
                                "subconditions": [
                                    {
                                        "match": False,
                                        "label": "check-success=continuous-integration/fake-ci",
                                        "description": None,
                                        "subconditions": [],
                                        "evaluations": [
                                            {
                                                "pull_request": 5678,
                                                "match": False,
                                                "evaluation_error": None,
                                                "related_checks": [
                                                    "continuous-integration/fake-ci"
                                                ],
                                            }
                                        ],
                                    },
                                    {
                                        "match": True,
                                        "label": "base=main",
                                        "description": None,
                                        "subconditions": [],
                                        "evaluations": [
                                            {
                                                "pull_request": 5678,
                                                "match": True,
                                                "evaluation_error": None,
                                                "related_checks": [],
                                            }
                                        ],
                                    },
                                ],
                                "evaluations": [],
                            },
                            "state": "success",
                        },
                        "queued_at": "2021-10-14T14:19:12+00:00",
                        "estimated_time_of_merge": "2021-10-14T15:19:12+00:00",
                    }
                }
            }
        },
        404: {"description": "The queue or the pull request is not found."},
    },
)
async def repository_queue_pull_request(
    owner: github_types.GitHubLogin = fastapi.Path(  # noqa: B008
        ..., description="The owner of the repository"
    ),
    repository: github_types.GitHubRepositoryName = fastapi.Path(  # noqa: B008
        ..., description="The name of the repository"
    ),
    queue_name: rules.QueueName = fastapi.Path(  # noqa: B008
        ..., description="The queue name"
    ),
    pr_number: github_types.GitHubPullRequestNumber = fastapi.Path(  # noqa: B008
        ..., description="The queued pull request number"
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
    queue_rules: rules.QueueRules = fastapi.Depends(get_queue_rules),  # noqa: B008
) -> EnhancedPullRequestQueued:
    async for train in merge_train.Train.iter_trains(repository_ctxt):
        try:
            queue_rule = queue_rules[queue_name]
        except KeyError:
            # The queue we seek is not in this train
            continue

        for position, (embarked_pull, car) in enumerate(train._iter_embarked_pulls()):
            if embarked_pull.user_pull_request_number != pr_number:
                continue

            mergeability_check = MergeabilityCheck.from_train_car(car)
            estimated_time_of_merge = await get_estimated_time_of_merge(
                train,
                queue_rules,
                embarked_pull,
                position,
                car,
            )

            return EnhancedPullRequestQueued(
                number=embarked_pull.user_pull_request_number,
                position=position,
                priority=embarked_pull.config["priority"],
                queue_rule=QueueRule(
                    name=embarked_pull.config["name"], config=queue_rule.config
                ),
                queued_at=embarked_pull.queued_at,
                mergeability_check=mergeability_check,
                estimated_time_of_merge=estimated_time_of_merge,
            )

    raise fastapi.HTTPException(
        status_code=404,
        detail="Pull request not found.",
    )


async def get_estimated_time_of_merge(
    train: merge_train.Train,
    queue_rules: rules.QueueRules,
    embarked_pull: merge_train.EmbarkedPull,
    embarked_pull_position: int,
    car: merge_train.TrainCar | None,
    previous_eta: datetime.datetime | None = None,
) -> datetime.datetime | None:
    queue_name = embarked_pull.config["name"]
    if await train.is_queue_frozen(queue_rules, queue_name):
        return None

    queue_checks_duration_stats = (
        await statistics_api.get_checks_duration_stats_for_queue(
            train.repository,
            queue_name,
            branch_name=train.ref,
        )
    )
    return await compute_estimated_time_of_merge(
        embarked_pull,
        embarked_pull_position,
        queue_rules[queue_name].config,
        queue_checks_duration_stats["median"],
        car,
        previous_eta,
    )


async def get_estimated_time_of_merge_from_stats(
    train: merge_train.Train,
    queue_rules: rules.QueueRules,
    embarked_pull: merge_train.EmbarkedPull,
    embarked_pull_position: int,
    checks_duration_stats: dict[rules.QueueName, statistics_api.ChecksDurationResponse],
    car: merge_train.TrainCar | None,
    previous_eta: datetime.datetime | None = None,
) -> datetime.datetime | None:
    queue_name = embarked_pull.config["name"]
    if await train.is_queue_frozen(queue_rules, queue_name):
        return None

    queue_checks_duration_stats = checks_duration_stats.get(
        queue_name,
        statistics_api.ChecksDurationResponse(mean=None, median=None),
    )
    return await compute_estimated_time_of_merge(
        embarked_pull,
        embarked_pull_position,
        queue_rules[queue_name].config,
        queue_checks_duration_stats["median"],
        car,
        previous_eta,
    )


async def compute_estimated_time_of_merge(
    embarked_pull: merge_train.EmbarkedPull,
    embarked_pull_position: int,
    queue_rule_config: rules.QueueConfig,
    checks_duration: float | None,
    car: merge_train.TrainCar | None,
    # previous_eta must be the eta of the pr in position `embarked_pull_position - 1`
    previous_eta: datetime.datetime | None = None,
) -> datetime.datetime | None:
    if checks_duration is None or (
        (car is None or car.last_conditions_evaluation is None) and previous_eta is None
    ):
        return None

    # `embarked_pull_position` starts at 0
    if previous_eta is not None and queue_utils.is_same_batch(
        embarked_pull_position,
        embarked_pull_position + 1,
        queue_rule_config["batch_size"],
    ):
        # The PR is in the same batch as the PR the `previous_eta` is from
        return previous_eta
    elif car is None or car.last_conditions_evaluation is None:
        # The PR is not in the same batch as the previous PR and has
        # no car.
        # mypy thinks previous_eta can be None, but the first `if` of this function prevents it.
        return previous_eta + datetime.timedelta(seconds=checks_duration)  # type: ignore[operator, return-value]

    if car.train_car_state.ci_started_at is None:
        return None

    raw_estimated_time_of_merge = (
        car.train_car_state.ci_started_at + datetime.timedelta(seconds=checks_duration)
    )
    # Evaluate schedule conditions relative to the current time
    farthest_datetime_from_conditions = (
        rules_conditions.get_farthest_datetime_from_non_match_schedule_condition(
            car.last_conditions_evaluation.subconditions,
            embarked_pull.user_pull_request_number,
            date.utcnow(),
        )
    )
    if farthest_datetime_from_conditions is None:
        # Only re-evaluate schedule conditions relative to the raw_estimated_time_of_merge
        # if the current time conditions do not fail.
        # If they do that means then we are already out of schedule, so no point in
        # trying to re-evaluate the schedule conditions relative to the raw_estimated_time_of_merge
        # since we won't return that time in the end.
        re_evaluated_conditions = rules_conditions.re_evaluate_schedule_conditions(
            car.last_conditions_evaluation.copy().subconditions,
            raw_estimated_time_of_merge,
        )
        farthest_datetime_from_conditions = (
            rules_conditions.get_farthest_datetime_from_non_match_schedule_condition(
                re_evaluated_conditions,
                embarked_pull.user_pull_request_number,
                raw_estimated_time_of_merge,
            )
        )

    max_number_of_pr_checked = (
        queue_rule_config["speculative_checks"] * queue_rule_config["batch_size"]
    )
    if farthest_datetime_from_conditions is not None:
        if embarked_pull_position < max_number_of_pr_checked:
            # Substract the checks_duration with the time the queued PR will have spent
            # waiting for the schedule only if it is part of some running speculative checks.
            checks_duration -= (
                farthest_datetime_from_conditions - embarked_pull.queued_at
            ).total_seconds()
            checks_duration = max(0, checks_duration)

        return farthest_datetime_from_conditions + datetime.timedelta(
            seconds=checks_duration
        )

    # No schedule conditions needs to be taken into account
    return raw_estimated_time_of_merge


@router.get(
    "/repos/{owner}/{repository}/queues/configuration",  # noqa: FS003
    summary="Get merge queues configuration",
    description="Get the list of all merge queues configuration sorted by processing order",
    response_model=QueuesConfig,
    responses={
        **api.default_responses,  # type: ignore
        422: {"description": "The configuration file is invalid."},
    },
)
async def repository_queues_configuration(
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> QueuesConfig:

    try:
        config = await repository_ctxt.get_mergify_config()
    except rules.InvalidRules:
        raise fastapi.HTTPException(
            status_code=422,
            detail="The configuration file is invalid.",
        )

    return QueuesConfig(
        [
            QueueRule(
                config=rule.config,
                name=rule.name,
            )
            for rule in config["queue_rules"]
        ]
    )


@router.put(
    "/repos/{owner}/{repository}/queue/{queue_name}/freeze",  # noqa: FS003
    summary="Freezes merge queue",
    description="Freezes the merge of the requested queue and the queues following it",
    response_model=QueueFreezeResponse,
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "The queue does not exist"},
    },
    dependencies=[fastapi.Depends(security.check_subscription_feature_queue_freeze)],
)
async def create_queue_freeze(
    queue_freeze_payload: QueueFreezePayload,
    application: application_mod.Application = fastapi.Security(  # noqa: B008
        security.get_application
    ),
    queue_name: rules.QueueName = fastapi.Path(  # noqa: B008
        ..., description="The name of the queue"
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> QueueFreezeResponse:

    if queue_freeze_payload.reason == "":
        queue_freeze_payload.reason = "No freeze reason was specified."

    try:
        config = await repository_ctxt.get_mergify_config()
    except rules.InvalidRules:
        raise fastapi.HTTPException(
            status_code=422,
            detail="The configuration file is invalid.",
        )

    queue_rules = config["queue_rules"]
    if all(queue_name != rule.name for rule in queue_rules):
        raise fastapi.HTTPException(
            status_code=404, detail=f'The queue "{queue_name}" does not exist.'
        )

    qf = await freeze.QueueFreeze.get(repository_ctxt, queue_name)
    if qf is None:
        qf = freeze.QueueFreeze(
            repository=repository_ctxt,
            name=queue_name,
            reason=queue_freeze_payload.reason,
            application_name=application.name,
            application_id=application.id,
            freeze_date=date.utcnow(),
            cascading=queue_freeze_payload.cascading,
        )

    if qf.reason != queue_freeze_payload.reason:
        qf.reason = queue_freeze_payload.reason

    if qf.cascading != queue_freeze_payload.cascading:
        qf.cascading = queue_freeze_payload.cascading

    await qf.save()
    return QueueFreezeResponse(
        queue_freezes=[
            QueueFreeze(
                name=qf.name,
                reason=qf.reason,
                application_name=qf.application_name,
                application_id=qf.application_id,
                freeze_date=qf.freeze_date,
                cascading=qf.cascading,
            )
        ],
    )


@router.delete(
    "/repos/{owner}/{repository}/queue/{queue_name}/freeze",  # noqa: FS003
    summary="Unfreeze merge queue",
    description="Unfreeze the specified merge queue",
    dependencies=[fastapi.Depends(security.check_subscription_feature_queue_freeze)],
    status_code=204,
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "The queue does not exist or is not currently frozen"},
    },
)
async def delete_queue_freeze(
    application: application_mod.Application = fastapi.Security(  # noqa: B008
        security.get_application
    ),
    queue_name: rules.QueueName = fastapi.Path(  # noqa: B008
        ..., description="The name of the queue"
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> fastapi.Response:

    qf = freeze.QueueFreeze(
        repository=repository_ctxt,
        name=queue_name,
        application_name=application.name,
        application_id=application.id,
    )
    if not await qf.delete():
        raise fastapi.HTTPException(
            status_code=404,
            detail=f'The queue "{queue_name}" does not exist or is not currently frozen.',
        )

    return fastapi.Response(status_code=HTTP_204_NO_CONTENT)


@router.get(
    "/repos/{owner}/{repository}/queue/{queue_name}/freeze",  # noqa: FS003
    summary="Get queue freeze data",
    description="Checks if the queue is frozen and get the queue freeze data",
    response_model=QueueFreezeResponse,
    dependencies=[fastapi.Depends(security.check_subscription_feature_queue_freeze)],
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "The queue does not exist or is not currently frozen"},
    },
)
async def get_queue_freeze(
    queue_name: rules.QueueName = fastapi.Path(  # noqa: B008
        ..., description="The name of the queue"
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> QueueFreezeResponse:

    qf = await freeze.QueueFreeze.get(repository_ctxt, queue_name)
    if qf is None:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f'The queue "{queue_name}" does not exist or is not currently frozen.',
        )

    return QueueFreezeResponse(
        queue_freezes=[
            QueueFreeze(
                name=qf.name,
                reason=qf.reason,
                application_name=qf.application_name,
                application_id=qf.application_id,
                freeze_date=qf.freeze_date,
                cascading=qf.cascading,
            )
        ],
    )


@router.get(
    "/repos/{owner}/{repository}/queues/freezes",  # noqa: FS003
    summary="Get the list of frozen queues",
    description="Get the list of frozen queues inside the requested repository",
    response_model=QueueFreezeResponse,
    dependencies=[fastapi.Depends(security.check_subscription_feature_queue_freeze)],
    responses={
        **api.default_responses,  # type: ignore
    },
)
async def get_list_queue_freeze(
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> QueueFreezeResponse:

    return QueueFreezeResponse(
        queue_freezes=[
            QueueFreeze(
                name=qf.name,
                reason=qf.reason,
                application_name=qf.application_name,
                application_id=qf.application_id,
                freeze_date=qf.freeze_date,
                cascading=qf.cascading,
            )
            async for qf in freeze.QueueFreeze.get_all(repository_ctxt)
        ]
    )
