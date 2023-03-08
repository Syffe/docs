from collections import abc
import dataclasses
import typing

import sqlalchemy
from sqlalchemy.dialects import postgresql
import sqlalchemy.exc
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.ci import models as ci_models
from mergify_engine.clients import github
from mergify_engine.models import github_actions as sql_models


class NotFoundError(Exception):
    pass


class PullRequestWorkflowRunPositionRegistry(typing.Protocol):
    async def get_job_run_position(
        self, pull_id: int, job_run: ci_models.JobRun
    ) -> int:
        ...


class PostgresPullRequestRegistry:
    async def insert(self, new_pull: ci_models.PullRequest) -> None:
        async with models.create_session() as session:
            sql = (
                postgresql.insert(sql_models.PullRequest)  # type: ignore [no-untyped-call]
                .values(id=new_pull.id, number=new_pull.number, title=new_pull.title)
                .on_conflict_do_update(
                    index_elements=[sql_models.PullRequest.id],
                    set_={"number": new_pull.number, "title": new_pull.title},
                )
            )
            await session.execute(sql)
            await session.commit()

    async def register_job_run(self, pull_id: int, job_run: ci_models.JobRun) -> None:
        async with models.create_session() as session:
            sql = (
                postgresql.insert(sql_models.PullRequestJobRunAssociation)  # type: ignore [no-untyped-call]
                .values(pull_request_id=pull_id, job_run_id=job_run.id)
                .on_conflict_do_nothing(
                    index_elements=["pull_request_id", "job_run_id"]
                )
            )
            await session.execute(sql)
            await session.commit()

    async def get_job_run_position(
        self, pull_id: int, job_run: ci_models.JobRun
    ) -> int:
        async with models.create_session() as session:
            sql = (
                sqlalchemy.select(
                    sql_models.JobRun.workflow_run_id,
                    sql_models.JobRun.started_at,
                )
                .join(sql_models.PullRequestJobRunAssociation.job_run)
                .where(
                    sql_models.PullRequestJobRunAssociation.pull_request_id == pull_id,
                    sql_models.JobRun.workflow_id == job_run.workflow_id,
                    sql_models.JobRun.name == job_run.name,
                )
            )
            result = await session.execute(sql)
            pull_workflow_runs: set[ci_models.WorkflowRun] = set()

            for row in result:
                pull_workflow_runs.add(
                    ci_models.WorkflowRun(
                        id=row.workflow_run_id,
                        started_at=row.started_at,
                    )
                )

            sorted_workflow_runs = sorted(
                pull_workflow_runs, key=lambda wr: wr.started_at
            )
            for i, workflow_run in enumerate(sorted_workflow_runs):
                if workflow_run.id == job_run.workflow_run_id:
                    return i

        raise NotFoundError(f"Workflow run `{job_run.workflow_run_id} not found")


@dataclasses.dataclass
class HTTPPullRequestRegistry:
    client: github.AsyncGithubClient
    cache_from_commit: dict[str, list[ci_models.PullRequest]] = dataclasses.field(
        init=False, repr=False, default_factory=dict
    )

    async def get_from_commit(
        self,
        owner: github_types.GitHubLogin,
        repository: github_types.GitHubRepositoryName,
        commit_sha: github_types.SHAType,
    ) -> list[ci_models.PullRequest]:
        if commit_sha in self.cache_from_commit:
            return self.cache_from_commit[commit_sha]

        # https://docs.github.com/en/rest/commits/commits#list-pull-requests-associated-with-a-commit
        pull_payloads = typing.cast(
            abc.AsyncIterable[github_types.GitHubPullRequest],
            self.client.items(
                f"/repos/{owner}/{repository}/commits/{commit_sha}/pulls",
                resource_name="pulls",
                page_limit=None,
            ),
        )
        pulls = [
            ci_models.PullRequest(id=p["id"], number=p["number"], title=p["title"])
            async for p in pull_payloads
        ]
        self.cache_from_commit[commit_sha] = pulls

        return pulls
