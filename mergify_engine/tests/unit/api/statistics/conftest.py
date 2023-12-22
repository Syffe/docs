import random
import typing

from _pytest import fixtures
import pytest
import sqlalchemy.ext.asyncio

from mergify_engine import context
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine.models import events as evt_models
from mergify_engine.models import events_metadata as evt_meta_models
from mergify_engine.models.github import repository as github_repository
from mergify_engine.rules.config import partition_rules


def get_request_param_evt_merged_defaults(
    pull_request: int,
    received_at: str,
    base_ref: str = "main",
    partition_names: list[str] | None = None,
    queue_name: str = "default",
) -> typing.Any:
    return (
        pull_request,
        received_at,
        base_ref,
        partition_names or ["default"],
        queue_name,
    )


@pytest.fixture()
async def _insert_merged_event(
    request: fixtures.SubRequest,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    fake_repository: context.Repository,
) -> None:
    repo = await github_repository.GitHubRepository.get_or_create(
        db,
        fake_repository.repo,
    )

    for row in request.param:
        (
            pull_request,
            received_at,
            base_ref,
            partition_names,
            queue_name,
        ) = get_request_param_evt_merged_defaults(
            **row,
        )
        db.add(
            evt_models.EventActionQueueMerged(
                repository=repo,
                pull_request=github_types.GitHubPullRequestNumber(pull_request),
                received_at=received_at,
                base_ref=github_types.GitHubRefType(base_ref),
                partition_names=[
                    partition_rules.PartitionRuleName(p) for p in partition_names
                ],
                queue_name=queue_name,
                queued_at=received_at,
                trigger="Rule: some rule",
                branch="hello world",
            ),
        )
    await db.commit()


def get_request_param_evt_checksend_defaults(
    pull_request: int,
    checks_started_at: str,
    checks_ended_at: str,
    received_at: str | None = None,
    base_ref: str = "main",
    partition_name: str = "default",
    queue_name: str = "default",
) -> typing.Any:
    return {
        "pull_request": pull_request,
        "checks_started_at": checks_started_at,
        "checks_ended_at": checks_ended_at,
        "received_at": received_at or checks_ended_at,
        "base_ref": base_ref,
        "partition_name": partition_name,
        "queue_name": queue_name,
    }


@pytest.fixture()
async def _insert_action_checks_end_event(
    request: fixtures.SubRequest,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    fake_repository: context.Repository,
) -> None:
    repo = await github_repository.GitHubRepository.get_or_create(
        db,
        fake_repository.repo,
    )

    for row in request.param:
        defaults = get_request_param_evt_checksend_defaults(**row)

        db.add(
            evt_models.EventActionQueueChecksEnd(
                repository=repo,
                pull_request=github_types.GitHubPullRequestNumber(
                    defaults["pull_request"],
                ),
                received_at=defaults["received_at"],
                base_ref=github_types.GitHubRefType(defaults["base_ref"]),
                trigger="Rule: some rule",
                branch="main",
                partition_name=defaults["partition_name"],
                position=1,
                queue_name=defaults["queue_name"],
                queued_at=date.utcnow(),
                aborted=False,
                abort_code=None,
                abort_reason=None,
                abort_status="DEFINITIVE",
                speculative_check_pull_request=evt_meta_models.SpeculativeCheckPullRequest(
                    number=random.randint(1, 100),
                    in_place=False,
                    checks_timed_out=False,
                    checks_conclusion="success",
                    unsuccessful_checks=[],
                    checks_started_at=defaults["checks_started_at"],
                    checks_ended_at=defaults["checks_ended_at"],
                ),
            ),
        )
    await db.commit()


def get_request_param_evt_queue_leave_defaults(
    pull_request: int,
    received_at: str,
    queued_at: str,
    seconds_waiting_for_freeze: int = 0,
    base_ref: str = "main",
    partition_name: str = "default",
    queue_name: str = "default",
) -> typing.Any:
    return {
        "pull_request": pull_request,
        "received_at": received_at,
        "queued_at": queued_at,
        "base_ref": base_ref,
        "partition_name": partition_name,
        "queue_name": queue_name,
        "seconds_waiting_for_freeze": seconds_waiting_for_freeze,
    }


@pytest.fixture()
async def _insert_action_queue_leave(
    request: fixtures.SubRequest,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    fake_repository: context.Repository,
) -> None:
    repo = await github_repository.GitHubRepository.get_or_create(
        db,
        fake_repository.repo,
    )

    for row in request.param:
        defaults = get_request_param_evt_queue_leave_defaults(**row)

        db.add(
            evt_models.EventActionQueueLeave(
                repository=repo,
                pull_request=github_types.GitHubPullRequestNumber(
                    defaults["pull_request"],
                ),
                received_at=defaults["received_at"],
                base_ref=github_types.GitHubRefType(defaults["base_ref"]),
                trigger="Rule: some rule",
                partition_name=defaults["partition_name"],
                queue_name=defaults["queue_name"],
                branch="main",
                position=1,
                queued_at=defaults["queued_at"],
                merged=True,
                reason="whatever",
                seconds_waiting_for_schedule=0,
                seconds_waiting_for_freeze=defaults["seconds_waiting_for_freeze"],
            ),
        )

    await db.commit()
