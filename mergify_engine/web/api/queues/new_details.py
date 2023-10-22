from __future__ import annotations

import dataclasses
import datetime
import typing

import fastapi
import pydantic

from mergify_engine import constants
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine.queue import merge_train
from mergify_engine.rules import conditions as rules_conditions
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web import api
from mergify_engine.web.api import security
from mergify_engine.web.api.queues import estimated_time_to_merge
from mergify_engine.web.api.queues import types


TRAIN_CAR_CHECKS_TYPE_MAPPING: dict[
    merge_train.TrainCarChecksType, typing.Literal["in_place", "draft_pr"]
] = {
    merge_train.TrainCarChecksType.INPLACE: "in_place",
    merge_train.TrainCarChecksType.DRAFT: "draft_pr",
    merge_train.TrainCarChecksType.DRAFT_DELEGATED: "draft_pr",
}

router = fastapi.APIRouter(
    tags=["queues"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


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
    continuous_integrations_ended_at: datetime.datetime | None = dataclasses.field(
        metadata={
            "description": "The timestamp when the CIs have ended for this pull request"
        }
    )
    continuous_integrations_state: typing.Literal[
        "pending", "success", "failed"
    ] = dataclasses.field(metadata={"description": "The combinated state of the CIs"})
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

        if car.train_car_state.checks_type in (
            merge_train.TrainCarChecksType.FAILED,
            None,
        ):
            return None

        if car.train_car_state.checks_type in (
            merge_train.TrainCarChecksType.DRAFT,
            merge_train.TrainCarChecksType.DRAFT_DELEGATED,
            merge_train.TrainCarChecksType.INPLACE,
        ):
            if car.queue_pull_request_number is None:
                raise RuntimeError(
                    f"car's checks type is {car.train_car_state.checks_type}, but queue_pull_request_number is None"
                )
            conditions_evaluation = (
                car.last_merge_conditions_evaluation.as_json_dict()
                if car.last_merge_conditions_evaluation is not None
                else None
            )
            return MergeabilityCheck(
                check_type=TRAIN_CAR_CHECKS_TYPE_MAPPING[
                    car.train_car_state.checks_type
                ],
                pull_request_number=car.queue_pull_request_number,
                started_at=car.train_car_state.ci_started_at,
                continuous_integrations_ended_at=car.train_car_state.ci_ended_at,
                continuous_integrations_state=car.train_car_state.ci_state.value,
                ended_at=car.checks_ended_timestamp,
                state=car.get_queue_check_run_conclusion().value or "pending",
                checks=car.last_checks,
                evaluated_conditions=car.last_evaluated_merge_conditions,
                conditions_evaluation=conditions_evaluation,
            )

        raise RuntimeError(
            f"Car's checks type unknown: {car.train_car_state.checks_type}"
        )


@pydantic.dataclasses.dataclass
class PullRequestSummary:
    title: str = dataclasses.field(
        metadata={"description": "The title of the pull request summary"}
    )
    unexpected_changes: str | None = dataclasses.field(
        metadata={"description": "The unexpected changes summary"}
    )
    freeze: str | None = dataclasses.field(
        metadata={"description": "The queue freeze summary"}
    )
    pause: str | None = dataclasses.field(
        metadata={"description": "The queue pause summary"}
    )
    checks_timeout: str | None = dataclasses.field(
        metadata={"description": "The checks timeout summary"}
    )
    batch_failure: str | None = dataclasses.field(
        metadata={"description": "The batch failure summary title"}
    )


@pydantic.dataclasses.dataclass
class EnhancedPullRequestQueued:
    number: github_types.GitHubPullRequestNumber = dataclasses.field(
        metadata={"description": "The number of the pull request"}
    )

    priority: int = dataclasses.field(
        metadata={"description": "The priority of this pull request"}
    )
    effective_priority: int = dataclasses.field(
        metadata={"description": "The effective_priority of this pull request"}
    )

    queue_rule: types.QueueRule = dataclasses.field(
        metadata={"description": "The queue rule associated to this pull request"}
    )

    queued_at: datetime.datetime = dataclasses.field(
        metadata={
            "description": "The timestamp when the pull requested has entered the queue"
        }
    )

    estimated_time_of_merge: datetime.datetime | None = dataclasses.field(
        metadata={
            "description": "The estimated timestamp when this pull request will be merged"
        }
    )

    positions: dict[partr_config.PartitionRuleName | None, int] = dataclasses.field(
        metadata={
            "description": (
                "The position of the pull request in the queue for each partitions "
                f"(if partitions are not used, the key will be `{partr_config.DEFAULT_PARTITION_NAME}`). "
                "The first pull request in the queue is at position 0"
            )
        },
        default_factory=dict,
    )

    mergeability_checks: dict[
        partr_config.PartitionRuleName | None, MergeabilityCheck | None
    ] = dataclasses.field(default_factory=dict)

    summary: dict[
        partr_config.PartitionRuleName | None, PullRequestSummary | None
    ] = dataclasses.field(default_factory=dict)

    partition_names: list[partr_config.PartitionRuleName] = dataclasses.field(
        metadata={
            "description": "The names of the partitions in which the pull request is queued in the specified queue"
        },
        default_factory=list,
    )


@router.get(
    "/new/queue/{queue_name}/pull/{pr_number}",
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
                        "partition_names": ["__default__"],
                        "positions": {
                            "__default__": 1,
                        },
                        "priority": 100,
                        "effective_priority": 100,
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
                                "commit_message_template": "",
                                "merge_bot_account": "",
                                "merge_method": "merge",
                                "update_bot_account": "",
                                "update_method": "rebase",
                            },
                        },
                        "mergeability_check": {
                            "__default__": {
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
                                    "schedule": None,
                                    "subconditions": [
                                        {
                                            "match": False,
                                            "label": "check-success=continuous-integration/fake-ci",
                                            "description": None,
                                            "schedule": None,
                                            "subconditions": [],
                                            "evaluations": [
                                                {
                                                    "pull_request": 5678,
                                                    "match": False,
                                                    "evaluation_error": None,
                                                    "related_checks": [
                                                        "continuous-integration/fake-ci"
                                                    ],
                                                    "next_evaluation_at": None,
                                                }
                                            ],
                                        },
                                        {
                                            "match": True,
                                            "label": "schedule=MON-FRI 12:00-15:00",
                                            "description": None,
                                            "schedule": date.Schedule.from_string(
                                                "MON-FRI 12:00-15:00"
                                            ).as_json_dict(),
                                            "subconditions": [],
                                            "evaluations": [
                                                {
                                                    "pull_request": 5678,
                                                    "match": True,
                                                    "evaluation_error": None,
                                                    "related_checks": [],
                                                    "next_evaluation_at": "2023-01-10T15:01:00+00:00",
                                                }
                                            ],
                                        },
                                        {
                                            "match": True,
                                            "label": "base=main",
                                            "description": None,
                                            "schedule": None,
                                            "subconditions": [],
                                            "evaluations": [
                                                {
                                                    "pull_request": 5678,
                                                    "match": True,
                                                    "evaluation_error": None,
                                                    "related_checks": [],
                                                    "next_evaluation_at": None,
                                                }
                                            ],
                                        },
                                    ],
                                    "evaluations": [],
                                },
                                "state": "success",
                            }
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
    queue_name: typing.Annotated[
        qr_config.QueueName, fastapi.Path(description="The queue name")
    ],
    pr_number: typing.Annotated[
        github_types.GitHubPullRequestNumber,
        fastapi.Path(description="The queued pull request number"),
    ],
    repository_ctxt: security.Repository,
    queue_rules: security.QueueRules,
    partition_rules: security.PartitionRules,
) -> EnhancedPullRequestQueued:
    try:
        queue_rule = queue_rules[queue_name]
    except KeyError:
        # The queue we seek is not defined in the configuration
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Queue `{queue_name}` does not exist.",
        )

    queued_pr = None
    async for convoy in merge_train.Convoy.iter_convoys(
        repository_ctxt, queue_rules, partition_rules
    ):
        for train in convoy.iter_trains():
            position, embarked_pull_with_car = train.find_embarked_pull(pr_number)
            if position is None or embarked_pull_with_car is None:
                # Only one check should be enough, doing both just to please mypy
                continue

            embarked_pull, car = embarked_pull_with_car

            if (
                embarked_pull.config["name"] != queue_name
                or embarked_pull.user_pull_request_number != pr_number
            ):
                continue

            if queued_pr is None:
                estimated_time_of_merge = await estimated_time_to_merge.get_estimation(
                    train,
                    embarked_pull,
                    position,
                    car,
                )

                queued_pr = EnhancedPullRequestQueued(
                    number=embarked_pull.user_pull_request_number,
                    queued_at=embarked_pull.queued_at,
                    estimated_time_of_merge=estimated_time_of_merge,
                    queue_rule=types.QueueRule(
                        name=queue_name, config=queue_rule.config
                    ),
                    priority=embarked_pull.config["priority"],
                    effective_priority=embarked_pull.config["effective_priority"],
                )

            summary = None
            if car is not None:
                checked_pull = car.get_checked_pull()
                raw_summary = car.get_original_pr_summary(checked_pull)
                summary = PullRequestSummary(
                    title=raw_summary.title,
                    unexpected_changes=raw_summary.unexpected_changes,
                    freeze=raw_summary.freeze,
                    pause=raw_summary.pause,
                    checks_timeout=raw_summary.checks_timeout,
                    batch_failure=raw_summary.get_batch_failure_summary_title(),
                )

            queued_pr.positions[train.partition_name] = position
            queued_pr.mergeability_checks[
                train.partition_name
            ] = MergeabilityCheck.from_train_car(car)
            queued_pr.partition_names.append(train.partition_name)
            queued_pr.summary[train.partition_name] = summary

    if queued_pr is None:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Pull request `{pr_number}` not found",
        )

    queued_pr.partition_names.sort()
    return queued_pr
