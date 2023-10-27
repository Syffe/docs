from __future__ import annotations

import typing

import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.event
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.hybrid

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.models.github import WorkflowJob
from mergify_engine.models.github import repository as gh_repository


COSINE_SIMILARITY_THRESHOLD = 0.9


class CiIssueCounter(models.Base):
    __tablename__ = "ci_issue_counter"
    __repr_attributes__: typing.ClassVar[tuple[str, ...]] = (
        "repository_id",
        "current_value",
    )

    repository_id: orm.Mapped[github_types.GitHubRepositoryIdType] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_repository.id"),
        anonymizer_config=None,
        primary_key=True,
    )

    current_value: orm.Mapped[int] = orm.mapped_column(anonymizer_config=None)

    @classmethod
    async def get_next_val(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository_id: github_types.GitHubRepositoryIdType,
    ) -> int:
        value = (
            await session.execute(
                sqlalchemy.update(cls)
                .values(current_value=cls.current_value + 1)
                .where(cls.repository_id == repository_id)
                .returning(cls.current_value)
            )
        ).scalar_one_or_none()

        if value is None:
            counter = cls(repository_id=repository_id, current_value=1)
            session.add(counter)
            value = 1

        return value


class CiIssue(models.Base):
    __tablename__ = "ci_issue"

    __table_args__ = (
        sqlalchemy.UniqueConstraint(
            "repository_id",
            "short_id_suffix",
        ),
    )
    __repr_attributes__: typing.ClassVar[tuple[str, ...]] = ("id", "short_id_suffix")

    id: orm.Mapped[int] = orm.mapped_column(
        primary_key=True,
        autoincrement=True,
        anonymizer_config=None,
    )

    short_id_suffix: orm.Mapped[int] = orm.mapped_column(anonymizer_config=None)

    name: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    repository_id: orm.Mapped[github_types.GitHubRepositoryIdType] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_repository.id"),
        anonymizer_config=None,
    )

    repository: orm.Mapped[gh_repository.GitHubRepository] = orm.relationship(
        lazy="joined"
    )

    jobs: orm.Mapped[list[WorkflowJob]] = orm.relationship(
        "WorkflowJob", back_populates="ci_issue", lazy="raise_on_sql"
    )

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository_id: github_types.GitHubRepositoryIdType,
    ) -> CiIssue:
        issue = cls(
            repository_id=repository_id,
            repository=await session.get_one(
                gh_repository.GitHubRepository, repository_id
            ),  # NOTE(Kntrolix): we get the repository during insert to mimic the lazy join that whould have been done on select
            short_id_suffix=await CiIssueCounter.get_next_val(session, repository_id),
        )
        session.add(issue)
        return issue

    @property
    def short_id(self) -> str:
        prefix = self.repository.name.upper()
        return f"{prefix}-{self.short_id_suffix}"

    @classmethod
    async def link_job_to_ci_issue(
        cls, session: sqlalchemy.ext.asyncio.AsyncSession, job: WorkflowJob
    ) -> None:
        if job.log_embedding is None:
            raise RuntimeError("link_job_to_ci_issue called with a pending job")

        if job.ci_issue_id is not None:
            raise RuntimeError(
                "link_job_to_ci_issue called with a job already linked to an issue"
            )

        stmt = (
            sqlalchemy.select(cls)
            .join(WorkflowJob, WorkflowJob.ci_issue_id == cls.id)
            .where(
                WorkflowJob.name_without_matrix == job.name_without_matrix,
                WorkflowJob.ci_issue_id.isnot(None),
                WorkflowJob.repository_id == job.repository_id,
                (1 - (WorkflowJob.log_embedding.cosine_distance(job.log_embedding)))
                >= COSINE_SIMILARITY_THRESHOLD,
            )
            .order_by(
                (
                    1 - (WorkflowJob.log_embedding.cosine_distance(job.log_embedding))
                ).desc()
            )
            .limit(1)
        )

        issue = (await session.execute(stmt)).scalar_one_or_none()

        if issue is None:
            issue = await CiIssue.insert(session, job.repository_id)

        job.ci_issue = issue
        job.ci_issue_id = issue.id
