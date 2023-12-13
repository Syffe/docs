import typing

from _pytest import fixtures
import pytest
import sqlalchemy.ext.asyncio

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine.models import events as evt_models
from mergify_engine.models.github import repository as github_repository
from mergify_engine.rules.config import partition_rules


def get_request_param_with_defaults(
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
        ) = get_request_param_with_defaults(
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
