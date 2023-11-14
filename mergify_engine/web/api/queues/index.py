from __future__ import annotations

import dataclasses
import datetime
import typing

import fastapi
import pydantic

from mergify_engine import constants
from mergify_engine import database
from mergify_engine import github_types
from mergify_engine.queue import merge_train
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.web import api
from mergify_engine.web.api import security
from mergify_engine.web.api.queues import details
from mergify_engine.web.api.queues import estimated_time_to_merge
from mergify_engine.web.api.queues import types
from mergify_engine.web.api.statistics import utils as web_stat_utils


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

        if car.train_car_state.checks_type in (
            merge_train.TrainCarChecksType.FAILED,
            None,
        ):
            return None

        if car.train_car_state.checks_type in (
            merge_train.TrainCarChecksType.DRAFT,
            merge_train.TrainCarChecksType.INPLACE,
            merge_train.TrainCarChecksType.DRAFT_DELEGATED,
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
                evaluated_conditions=car.last_evaluated_merge_conditions,
            )

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
        mergeability_check = details.MergeabilityCheck.from_train_car(car)
        if mergeability_check is not None:
            return cls(**dataclasses.asdict(mergeability_check))
        return None


@pydantic.dataclasses.dataclass
class PullRequestQueued:
    number: github_types.GitHubPullRequestNumber = pydantic.Field(
        description="The number of the pull request"
    )
    position: int = pydantic.Field(
        description="The position of the pull request in the queue. The first pull request in the queue is at position 0"
    )
    priority: int = pydantic.Field(description="The priority of this pull request")
    effective_priority: int = pydantic.Field(
        description="The effective priority of this pull request"
    )
    queue_rule: types.QueueRule = pydantic.Field(
        description="The queue rule associated to this pull request"
    )
    queued_at: datetime.datetime = pydantic.Field(
        description="The timestamp when the pull requested has entered in the queue"
    )
    speculative_check_pull_request: SpeculativeCheckPullRequest | None = pydantic.Field(
        ...,
        description="Use `mergeability_check` instead",
        json_schema_extra={"deprecated": True},
    )
    mergeability_check: BriefMergeabilityCheck | None = pydantic.Field(
        description="Information about the mergeability check currently processed"
    )
    estimated_time_of_merge: datetime.datetime | None = pydantic.Field(
        description="The estimated timestamp when this pull request will be merged"
    )
    partition_name: partr_config.PartitionRuleName = pydantic.Field(
        description="The name of the partition, if any, in which the pull request is queued"
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
class Queues:
    queues: list[Queue] = dataclasses.field(
        default_factory=list, metadata={"description": "The queues of the repository"}
    )


@router.get(
    "/queues",
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
                                        "effective_priority": 100,
                                        "partition_name": partr_config.DEFAULT_PARTITION_NAME,
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
                                                "autosquash": True,
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
                                        "effective_priority": 100,
                                        "partition_name": partr_config.DEFAULT_PARTITION_NAME,
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
                                                "autosquash": False,
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
    session: database.Session,
    repository_ctxt: security.Repository,
    queue_rules: security.QueueRules,
    partition_rules: security.PartitionRules,
) -> Queues:
    queue_names = tuple(rule.name for rule in queue_rules)
    if partition_rules:
        partition_names = tuple(rule.name for rule in partition_rules)
    else:
        partition_names = (partr_config.DEFAULT_PARTITION_NAME,)

    stats = await web_stat_utils.get_queue_check_durations_per_partition_queue_branch(
        session, repository_ctxt, partition_rules, partition_names, queue_names
    )

    queues = Queues()
    async for convoy in merge_train.Convoy.iter_convoys(
        repository_ctxt, queue_rules, partition_rules
    ):
        for train in convoy.iter_trains():
            queue = Queue(Branch(train.convoy.ref))
            previous_eta = None
            previous_car = None
            for position, (embarked_pull, car) in enumerate(
                train._iter_embarked_pulls()
            ):
                try:
                    queue_rule = queue_rules[embarked_pull.config["name"]]
                except KeyError:
                    # This car is going to be deleted so skip it
                    continue

                speculative_check_pull_request = (
                    SpeculativeCheckPullRequest.from_train_car(car)
                )

                previous_eta = (
                    estimated_time_of_merge
                ) = await estimated_time_to_merge.get_estimation_from_stats(
                    train,
                    embarked_pull,
                    position,
                    stats,
                    car,
                    previous_eta,
                    previous_car,
                )
                previous_car = car

                queue.pull_requests.append(
                    PullRequestQueued(
                        number=embarked_pull.user_pull_request_number,
                        position=position,
                        priority=embarked_pull.config["priority"],
                        effective_priority=embarked_pull.config["effective_priority"],
                        queue_rule=types.QueueRule(
                            name=embarked_pull.config["name"], config=queue_rule.config
                        ),
                        queued_at=embarked_pull.queued_at,
                        speculative_check_pull_request=speculative_check_pull_request,
                        mergeability_check=BriefMergeabilityCheck.from_train_car(car),
                        estimated_time_of_merge=estimated_time_of_merge,
                        partition_name=train.partition_name,
                    )
                )

        queues.queues.append(queue)

    return queues
