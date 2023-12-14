from __future__ import annotations

import dataclasses
import datetime  # noqa: TCH003
import typing

import fastapi
import pydantic

from mergify_engine import constants
from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine.queue import merge_train
from mergify_engine.rules import conditions as rules_conditions  # noqa: TCH001
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.web import api
from mergify_engine.web.api import security
from mergify_engine.web.api.queues import estimated_time_to_merge
from mergify_engine.web.api.queues import types
from mergify_engine.web.api.statistics import utils as web_stat_utils


TRAIN_CAR_CHECKS_TYPE_MAPPING: dict[
    merge_train.TrainCarChecksType,
    typing.Literal["in_place", "draft_pr"],
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
        metadata={"description": "The type of queue check (in_place or draft_pr)"},
    )
    pull_request_number: github_types.GitHubPullRequestNumber = dataclasses.field(
        metadata={
            "description": "The number of the pull request used by the speculative check",
        },
    )
    started_at: datetime.datetime | None = dataclasses.field(
        metadata={
            "description": "The timestamp when the checks have started for this pull request",
        },
    )
    ended_at: datetime.datetime | None = dataclasses.field(
        metadata={
            "description": "The timestamp when the checks have ended for this pull request",
        },
    )
    continuous_integrations_ended_at: datetime.datetime | None = dataclasses.field(
        metadata={
            "description": "The timestamp when the CIs have ended for this pull request",
        },
    )
    continuous_integrations_state: typing.Literal[
        "pending",
        "success",
        "failed",
    ] = dataclasses.field(metadata={"description": "The combinated state of the CIs"})
    checks: list[merge_train.QueueCheck] = dataclasses.field(
        metadata={"description": "The list of pull request checks"},
    )
    evaluated_conditions: str = dataclasses.field(
        metadata={"description": "The queue rule conditions evaluation report"},
    )
    state: merge_train.CheckStateT = dataclasses.field(
        metadata={"description": "The global state of the checks"},
    )
    conditions_evaluation: (
        rules_conditions.QueueConditionEvaluationJsonSerialized | None
    ) = dataclasses.field(
        metadata={"description": "The queue rule conditions evaluation"},
    )

    @classmethod
    def from_train_car(
        cls,
        car: merge_train.TrainCar | None,
        user_pull_request_number: github_types.GitHubPullRequestNumber,
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
                    f"car's checks type is {car.train_car_state.checks_type}, but queue_pull_request_number is None",
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
                state=car.get_queue_check_run_conclusion(user_pull_request_number).value
                or "pending",
                checks=car.last_checks,
                evaluated_conditions=car.last_evaluated_merge_conditions,
                conditions_evaluation=conditions_evaluation,
            )

        raise RuntimeError(
            f"Car's checks type unknown: {car.train_car_state.checks_type}",
        )


@pydantic.dataclasses.dataclass
class PullRequestSummary:
    title: str = dataclasses.field(
        metadata={"description": "The title of the pull request summary"},
    )
    unexpected_changes: str | None = dataclasses.field(
        metadata={"description": "The unexpected changes summary"},
    )
    freeze: str | None = dataclasses.field(
        metadata={"description": "The queue freeze summary"},
    )
    checks_timeout: str | None = dataclasses.field(
        metadata={"description": "The checks timeout summary"},
    )
    batch_failure: str | None = dataclasses.field(
        metadata={"description": "The batch failure summary title"},
    )


@pydantic.dataclasses.dataclass
class EnhancedPullRequestQueued:
    number: github_types.GitHubPullRequestNumber = dataclasses.field(
        metadata={"description": "The number of the pull request"},
    )

    position: int = dataclasses.field(
        metadata={
            "description": "The position of the pull request in the queue. The first pull request in the queue is at position 0",
        },
    )

    priority: int = dataclasses.field(
        metadata={"description": "The priority of this pull request"},
    )
    effective_priority: int = dataclasses.field(
        metadata={"description": "The effective priority of this pull request"},
    )
    queue_rule: types.QueueRule = dataclasses.field(
        metadata={"description": "The queue rule associated to this pull request"},
    )

    queued_at: datetime.datetime = dataclasses.field(
        metadata={
            "description": "The timestamp when the pull requested has entered in the queue",
        },
    )
    mergeability_check: MergeabilityCheck | None

    estimated_time_of_merge: datetime.datetime | None = dataclasses.field(
        metadata={
            "description": "The estimated timestamp when this pull request will be merged",
        },
    )
    summary: PullRequestSummary | None = dataclasses.field(
        metadata={"description": "The pull request summary"},
    )


@router.get(
    "/queue/{queue_name}/pull/{pr_number}",
    include_in_schema=False,
    summary="Get a queued pull request",
    description="Get a pull request queued in a merge queue of a repository",
    response_model=EnhancedPullRequestQueued,
    responses={
        **api.default_responses,  # type: ignore[dict-item]
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
                                "commit_message_template": "",
                                "merge_bot_account": "",
                                "merge_method": "merge",
                                "update_bot_account": "",
                                "update_method": "rebase",
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
                                                    "continuous-integration/fake-ci",
                                                ],
                                                "next_evaluation_at": None,
                                            },
                                        ],
                                    },
                                    {
                                        "match": True,
                                        "label": "schedule=MON-FRI 12:00-15:00",
                                        "description": None,
                                        "schedule": date.Schedule.from_string(
                                            "MON-FRI 12:00-15:00",
                                        ).as_json_dict(),
                                        "subconditions": [],
                                        "evaluations": [
                                            {
                                                "pull_request": 5678,
                                                "match": True,
                                                "evaluation_error": None,
                                                "related_checks": [],
                                                "next_evaluation_at": "2023-01-10T15:01:00+00:00",
                                            },
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
                                            },
                                        ],
                                    },
                                ],
                                "evaluations": [],
                            },
                            "state": "success",
                        },
                        "queued_at": "2021-10-14T14:19:12+00:00",
                        "estimated_time_of_merge": "2021-10-14T15:19:12+00:00",
                        "summary": {
                            "title": "The pull request is embarked for merge",
                            "unexpected_changes": None,
                            "freeze": None,
                            "checks_timeout": None,
                            "batch_failure": None,
                        },
                    },
                },
            },
        },
        404: {"description": "The queue or the pull request is not found."},
    },
)
async def repository_queue_pull_request(
    session: database.Session,
    pr_number: typing.Annotated[
        github_types.GitHubPullRequestNumber,
        fastapi.Path(description="The queued pull request number"),
    ],
    repository_ctxt: security.RepositoryWithConfig,
    queue_rule: security.QueueRuleByNameFromPath,
) -> EnhancedPullRequestQueued:
    partition_rules = repository_ctxt.mergify_config["partition_rules"]
    queue_rules = repository_ctxt.mergify_config["queue_rules"]

    stats = await web_stat_utils.get_queue_check_durations_per_partition_queue_branch(
        session,
        repository_ctxt.repo["id"],
        partition_rules.names,
        queue_rules.names,
    )

    async for convoy in merge_train.Convoy.iter_convoys(
        repository_ctxt,
    ):
        for train in convoy.iter_trains():
            if train.partition_name != partr_config.DEFAULT_PARTITION_NAME:
                continue

            for position, (embarked_pull, car) in enumerate(
                train._iter_embarked_pulls(),
            ):
                if embarked_pull.user_pull_request_number != pr_number:
                    continue

                mergeability_check = MergeabilityCheck.from_train_car(
                    car,
                    embarked_pull.user_pull_request_number,
                )
                estimated_time_of_merge = (
                    await estimated_time_to_merge.get_estimation_from_stats(
                        train,
                        embarked_pull,
                        position,
                        stats,
                        car,
                    )
                )

                if car is not None:
                    raw_summary = car.get_original_pr_summary(
                        embarked_pull.user_pull_request_number,
                    )
                    summary = PullRequestSummary(
                        title=raw_summary.title,
                        freeze=raw_summary.freeze,
                        checks_timeout=raw_summary.checks_timeout,
                        batch_failure=raw_summary.get_batch_failure_summary_title(),
                        # TODO(sileht): We need to drop it first from the dashboard
                        unexpected_changes=None,
                    )
                else:
                    summary = None

                return EnhancedPullRequestQueued(
                    number=embarked_pull.user_pull_request_number,
                    position=position,
                    priority=embarked_pull.config["priority"],
                    effective_priority=embarked_pull.config["effective_priority"],
                    queue_rule=types.QueueRule(
                        name=embarked_pull.config["name"],
                        config=queue_rule.config,
                    ),
                    queued_at=embarked_pull.queued_at,
                    mergeability_check=mergeability_check,
                    estimated_time_of_merge=estimated_time_of_merge,
                    summary=summary,
                )

    raise fastapi.HTTPException(
        status_code=404,
        detail="Pull request not found.",
    )
