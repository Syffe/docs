import asyncio
from collections import abc
import dataclasses
import datetime
import re
import typing

import daiquiri
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
import sqlalchemy.ext.asyncio

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine.ci import models as ci_models
from mergify_engine.ci import pull_registries
from mergify_engine.clients import github
from mergify_engine.models import github_account
from mergify_engine.models import github_actions as sql_models


LOG = daiquiri.getLogger(__name__)


class RunnerProperties(typing.NamedTuple):
    operating_system: ci_models.OperatingSystem
    cores: int

    @classmethod
    def unknown(cls) -> "RunnerProperties":
        return cls("Unknown", 0)


class JobRegistry(typing.Protocol):
    def search(
        self,
        owner: str,
        repository: str | None,
        start_at: datetime.date | None,
        end_at: datetime.date | None,
    ) -> abc.AsyncIterator[ci_models.JobRun]:
        ...

    def get_job_running_order(self, pull_id: int, job_run: ci_models.JobRun) -> int:
        ...


@dataclasses.dataclass
class PostgresJobRegistry:
    _job_running_order_cache: dict[tuple[int, int], int] = dataclasses.field(
        default_factory=dict
    )

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

    async def search(
        self,
        owner: str,
        repository: str | None,
        start_at: datetime.date | None,
        end_at: datetime.date | None,
    ) -> abc.AsyncIterator[ci_models.JobRun]:
        _, records = await asyncio.gather(
            self._fill_job_running_order_cache(owner, repository, start_at, end_at),
            self._execute_search(owner, repository, start_at, end_at),
        )

        for record in records:
            job = ci_models.JobRun(
                id=record.id,
                workflow_run_id=record.workflow_run_id,
                workflow_id=record.workflow_id,
                name=record.name,
                owner=ci_models.Account(
                    id=record.owner.id,
                    login=github_types.GitHubLogin(record.owner.login),
                ),
                repository=record.repository,
                conclusion=record.conclusion.value,
                triggering_event=record.triggering_event.value,
                triggering_actor=ci_models.Account(
                    id=record.triggering_actor.id,
                    login=github_types.GitHubLogin(record.triggering_actor.login),
                ),
                started_at=record.started_at,
                completed_at=record.completed_at,
                pulls=[
                    ci_models.PullRequest(
                        id=pr.id, number=pr.number, title=pr.title, state=pr.state
                    )
                    for pr in record.pull_requests
                ],
                run_attempt=record.run_attempt,
                operating_system=record.operating_system.value,
                cores=record.cores,
            )
            yield job

    async def _execute_search(
        self,
        owner: str,
        repository: str | None,
        start_at: datetime.date | None,
        end_at: datetime.date | None,
    ) -> sqlalchemy.ScalarResult[sql_models.JobRun]:
        async with database.create_session() as session:
            sql = (
                sqlalchemy.select(sql_models.JobRun)
                .join(
                    github_account.GitHubAccount,
                    github_account.GitHubAccount.id == sql_models.JobRun.owner_id,
                )
                .where(*self._create_filter(owner, repository, start_at, end_at))
            )
            return await session.scalars(sql)

    async def _fill_job_running_order_cache(
        self,
        owner: str,
        repository: str | None,
        start_at: datetime.date | None,
        end_at: datetime.date | None,
    ) -> None:
        """Fills the PR-job cache.

        This cache contains the running order for each job.

        Say we have a PR "p1" created at 12 a.m. and updated at 1 p.m., with one
        workflow "w1" containing two jobs ("tests" and "linters"). We have to
        calculate the running order like the following example.

        +-----------------+-------------+----------+---------------------+----------+
        | pull_request_id | workflow_id | job_name |   job_started_at    | position |
        +-----------------+-------------+----------+---------------------+----------+
        | p1              | w1          | tests    | 2023-03-30 12:00:00 |        1 |
        | p1              | w1          | tests    | 2023-03-30 13:00:00 |        2 |
        | p1              | w1          | linters  | 2023-03-30 12:00:00 |        1 |
        | p1              | w1          | linters  | 2023-03-30 13:00:00 |        2 |
        +-----------------+-------------+----------+---------------------+----------+
        """
        async with database.create_session() as session:
            result = await session.execute(
                self._job_running_order_sql(owner, repository, start_at, end_at)
            )
            for row in result:
                pull_job_association: sql_models.PullRequestJobRunAssociation = row[0]
                position: int = row[1]
                self._job_running_order_cache[
                    (
                        pull_job_association.pull_request_id,
                        pull_job_association.job_run_id,
                    )
                ] = position

    def _job_running_order_sql(
        self,
        owner: str,
        repository: str | None,
        start_at: datetime.date | None,
        end_at: datetime.date | None,
    ) -> sqlalchemy.Select[tuple[sql_models.PullRequestJobRunAssociation, int]]:
        PullRequestJobRunAssociation1 = orm.aliased(
            sql_models.PullRequestJobRunAssociation
        )
        PullRequestJobRunAssociation2 = orm.aliased(
            sql_models.PullRequestJobRunAssociation
        )
        JobRun1 = orm.aliased(sql_models.JobRun)
        JobRun2 = orm.aliased(sql_models.JobRun)
        subquery = (
            sqlalchemy.select(PullRequestJobRunAssociation1)
            .join(JobRun1)
            .join(
                github_account.GitHubAccount,
                github_account.GitHubAccount.id == JobRun1.owner_id,
            )
            .where(
                *self._create_filter(
                    owner, repository, start_at, end_at, job_model=JobRun1
                ),
                PullRequestJobRunAssociation1.pull_request_id
                == PullRequestJobRunAssociation2.pull_request_id,
            )
            .exists()
        )
        return (
            sqlalchemy.select(
                PullRequestJobRunAssociation2,
                sqlalchemy.func.row_number().over(  # type: ignore [no-untyped-call]
                    partition_by=(
                        PullRequestJobRunAssociation2.pull_request_id,
                        JobRun2.workflow_id,
                        JobRun2.name,
                    ),
                    order_by=JobRun2.started_at,
                ),
            )
            .join(JobRun2)
            .where(subquery, JobRun2.run_attempt == 1)
        )

    def _create_filter(
        self,
        owner: str,
        repository: str | None,
        start_at: datetime.date | None,
        end_at: datetime.date | None,
        job_model: type[sql_models.JobRun] = sql_models.JobRun,
    ) -> list[sqlalchemy.ColumnElement[bool]]:
        filter_ = [
            sqlalchemy.func.lower(github_account.GitHubAccount.login) == owner.lower()
        ]
        if repository is not None:
            filter_.append(job_model.repository == repository)
        if start_at is not None:
            filter_.append(job_model.started_at >= start_at)
        if end_at is not None:
            filter_.append(job_model.started_at < end_at + datetime.timedelta(days=1))

        return filter_

    def get_job_running_order(self, pull_id: int, job_run: ci_models.JobRun) -> int:
        return self._job_running_order_cache[pull_id, job_run.id]


@dataclasses.dataclass
class HTTPJobRegistry:
    client: github.AsyncGithubClient
    pull_registry: pull_registries.PullRequestFromCommitRegistry
    destination_registry: PostgresJobRegistry

    async def search(
        self, owner: str, repository: str, at: datetime.date
    ) -> abc.AsyncGenerator[ci_models.JobRun, None]:
        # https://docs.github.com/en/rest/actions/workflow-runs?apiVersion=2022-11-28#list-workflow-runs-for-a-repository
        http_runs = [
            typing.cast(github_types.GitHubWorkflowRun, run)
            async for run in self.client.items(
                f"/repos/{owner}/{repository}/actions/runs",
                resource_name="runs",
                page_limit=None,
                list_items="workflow_runs",
                params={"created": at.isoformat()},
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

            yield await self.create_job(self.pull_registry, job, run)

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

    @classmethod
    async def create_job(
        cls,
        pull_registry: pull_registries.PullRequestFromCommitRegistry,
        job_payload: github_types.GitHubJobRun,
        run_payload: github_types.GitHubWorkflowRun,
    ) -> ci_models.JobRun:
        runner_properties = cls._extract_runner_properties(job_payload)

        return ci_models.JobRun(
            id=job_payload["id"],
            workflow_run_id=run_payload["id"],
            workflow_id=run_payload["workflow_id"],
            name=job_payload["name"],
            owner=ci_models.Account(
                id=run_payload["repository"]["owner"]["id"],
                login=run_payload["repository"]["owner"]["login"],
            ),
            repository=run_payload["repository"]["name"],
            conclusion=job_payload["conclusion"],
            triggering_event=run_payload["event"],
            triggering_actor=ci_models.Account(
                id=run_payload["triggering_actor"]["id"],
                login=run_payload["triggering_actor"]["login"],
            ),
            started_at=datetime.datetime.fromisoformat(job_payload["started_at"]),
            completed_at=datetime.datetime.fromisoformat(job_payload["completed_at"]),
            pulls=await pull_registry.get_from_commit(
                run_payload["repository"]["owner"]["login"],
                run_payload["repository"]["name"],
                run_payload["head_sha"],
            ),
            run_attempt=run_payload["run_attempt"],
            operating_system=runner_properties.operating_system,
            cores=runner_properties.cores,
        )

    @staticmethod
    def _extract_runner_properties(
        job_payload: github_types.GitHubJobRun,
    ) -> RunnerProperties:
        for label in job_payload["labels"]:
            try:
                return HTTPJobRegistry._extract_runner_properties_from_label(label)
            except ValueError:
                continue

        return RunnerProperties.unknown()

    @staticmethod
    def _extract_runner_properties_from_label(label: str) -> RunnerProperties:
        # NOTE(charly): https://docs.github.com/en/actions/using-github-hosted-runners/about-github-hosted-runners#supported-runners-and-hardware-resources
        match = re.match(r"(ubuntu|windows|macos)-[\w\.]+(-xl)?(-(\d+)-cores)?", label)
        if not match:
            raise ValueError(f"Cannot parse label '{label}'")

        raw_os, _, _, raw_cores = match.groups()

        operating_system: ci_models.OperatingSystem
        if raw_os == "ubuntu":
            operating_system = "Linux"
        elif raw_os == "windows":
            operating_system = "Windows"
        elif raw_os == "macos":
            operating_system = "macOS"
        else:
            raise ValueError(f"Unknown operating system '{operating_system}'")

        if raw_cores is not None:
            cores = int(raw_cores)
        elif operating_system == "macOS":
            cores = 3
        else:
            cores = 2

        return RunnerProperties(operating_system, cores)
