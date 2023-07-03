from collections import abc
import dataclasses
import typing

import daiquiri
import sqlalchemy
from sqlalchemy.dialects import postgresql
import sqlalchemy.ext.asyncio

from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine.ci import models as ci_models
from mergify_engine.ci import pull_registries
from mergify_engine.clients import github
from mergify_engine.models import github_account
from mergify_engine.models import github_actions as sql_models


LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class PostgresJobRegistry:
    _job_running_order_cache: dict[tuple[int, int], int] = dataclasses.field(
        default_factory=dict
    )

    async def exists(self, job_id: int) -> bool:
        async with database.create_session() as session:
            sql = sqlalchemy.select(
                sqlalchemy.exists().where(sql_models.JobRun.id == job_id)
            )
            return await session.scalar(sql) or False

    async def filter_if_exist(self, *run_ids: int) -> set[int]:
        async with database.create_session() as session:
            sql = sqlalchemy.select(sql_models.JobRun.workflow_run_id).where(
                sql_models.JobRun.workflow_run_id.in_(run_ids)
            )
            existing_run_ids = set(await session.scalars(sql))
            return set(run_ids) - existing_run_ids

    async def insert(self, job_run: ci_models.JobRun) -> None:
        async with database.create_session() as session:
            await self._insert_account(session, job_run.owner)
            await self._insert_account(session, job_run.triggering_actor)

            sql = (
                postgresql.insert(sql_models.JobRun)  # type: ignore [no-untyped-call]
                .values(
                    id=job_run.id,
                    workflow_run_id=job_run.workflow_run_id,
                    workflow_id=job_run.workflow_id,
                    name=job_run.name,
                    owner_id=job_run.owner.id,
                    repository=job_run.repository,
                    conclusion=sql_models.JobRunConclusion(job_run.conclusion),
                    triggering_event=sql_models.JobRunTriggerEvent(
                        job_run.triggering_event
                    ),
                    triggering_actor_id=job_run.triggering_actor.id,
                    started_at=job_run.started_at,
                    completed_at=job_run.completed_at,
                    run_attempt=job_run.run_attempt,
                    operating_system=sql_models.JobRunOperatingSystem(
                        job_run.operating_system
                    ),
                    cores=job_run.cores,
                )
                .on_conflict_do_nothing(index_elements=["id"])
            )
            await session.execute(sql)
            await session.commit()

    @staticmethod
    async def _insert_account(
        session: sqlalchemy.ext.asyncio.AsyncSession, account: ci_models.Account
    ) -> None:
        await github_account.GitHubAccount.create_or_update(
            session, account.id, account.login
        )


@dataclasses.dataclass
class HTTPJobRegistry:
    client: github.AsyncGitHubClient
    pull_registry: pull_registries.PullRequestFromCommitRegistry
    destination_registry: PostgresJobRegistry

    async def search(
        self, owner: str, repository: str, date_range: date.DateTimeRange
    ) -> abc.AsyncGenerator[ci_models.JobRun, None]:
        # https://docs.github.com/en/rest/actions/workflow-runs?apiVersion=2022-11-28#list-workflow-runs-for-a-repository
        http_runs = [
            typing.cast(github_types.GitHubWorkflowRun, run)
            async for run in self.client.items(
                f"/repos/{owner}/{repository}/actions/runs",
                resource_name="runs",
                page_limit=None,
                list_items="workflow_runs",
                params={"created": date_range.as_github_date_query()},
            )
        ]
        runs = {run["id"]: run for run in http_runs}
        missing_run_ids = await self.destination_registry.filter_if_exist(*runs.keys())

        for run_id in missing_run_ids:
            LOG.debug(f"workflow run {run_id}", gh_owner=owner, gh_repo=repository)

            async for job in self._get_jobs_from_url(runs[run_id]):
                yield job

    async def _get_jobs_from_url(
        self, run: github_types.GitHubWorkflowRun
    ) -> abc.AsyncGenerator[ci_models.JobRun, None]:
        owner = run["repository"]["owner"]["login"]
        repository = run["repository"]["name"]

        # https://docs.github.com/en/rest/actions/workflow-jobs?apiVersion=2022-11-28#list-jobs-for-a-workflow-run
        jobs = typing.cast(
            abc.AsyncIterable[github_types.GitHubJobRun],
            self.client.items(
                run["jobs_url"],
                resource_name="jobs",
                list_items="jobs",
                page_limit=None,
            ),
        )

        async for job in jobs:
            LOG.debug(f"job run {job['id']}", gh_owner=owner, gh_repo=repository)

            if self._is_ignored(run, job):
                LOG.info(
                    f"job run {job['id']} ignored",
                    gh_owner=owner,
                    gh_repo=repository,
                    workflow_run_payload=run,
                    job_payload=job,
                )
                continue

            yield await ci_models.JobRun.create_job(self.pull_registry, job, run)

    def _is_ignored(
        self, run: github_types.GitHubWorkflowRun, job: github_types.GitHubJobRun
    ) -> bool:
        return self.is_workflow_run_ignored(run) or self.is_workflow_job_ignored(job)

    @staticmethod
    def is_workflow_job_ignored(payload: github_types.GitHubJobRun) -> bool:
        return not payload["completed_at"] or payload["conclusion"] == "skipped"

    @staticmethod
    def is_workflow_run_ignored(payload: github_types.GitHubWorkflowRun) -> bool:
        if payload["conclusion"] is None:
            return True

        try:
            sql_models.JobRunTriggerEvent(payload["event"])
        except ValueError:
            return True
        else:
            return False
