from __future__ import annotations

import enum
import typing

import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.event
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.hybrid

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.models import github as gh_models
from mergify_engine.models.github import pull_request as gh_pull_request


COSINE_SIMILARITY_THRESHOLD = 0.97

MIN_MATCHING_SCORE = 6


class CiIssueStatus(enum.Enum):
    UNRESOLVED = "unresolved"
    RESOLVED = "resolved"


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
                .returning(cls.current_value),
            )
        ).scalar_one_or_none()

        if value is None:
            counter = cls(repository_id=repository_id, current_value=1)
            session.add(counter)
            value = 1

        return value


class CiIssueMixin:
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

    @orm.declared_attr
    def repository(self) -> orm.Mapped[gh_models.GitHubRepository]:
        return orm.relationship(lazy="joined")

    status: orm.Mapped[CiIssueStatus] = orm.mapped_column(
        sqlalchemy.Enum(CiIssueStatus),
        server_default=CiIssueStatus.UNRESOLVED.name,
        anonymizer_config="anon.random_in_enum(status)",
    )

    @sqlalchemy.ext.hybrid.hybrid_property
    def short_id(self) -> str:
        prefix = self.repository.name.upper()
        return f"{prefix}-{self.short_id_suffix}"

    @short_id.inplace.expression
    @classmethod
    def _short_id_expression(cls) -> sqlalchemy.ColumnElement[str]:
        """
        Warning: When using this field in a sql query, don't forget to specify
        the join clause, otherwise sqlachemy will assume that it's a cross-join.

        This:
            sqlalchemy.select(CiIssue.id, CiIssue.short_id)
        Will produce:
            SELECT
                ci_issue.id,
                concat(github_repository.name, '-', ci_issue.short_id_suffix) AS short_id
            FROM ci_issue, github_repository

        This:
            sqlalchemy.select(CiIssue.id, CiIssue.short_id).join(
            gh_models.GitHubRepository,
            gh_models.GitHubRepository.id == CiIssue.repository_id,
        )
        Will produce:
            SELECT
                ci_issue.id,
                concat(github_repository.name, '-', ci_issue.short_id_suffix) AS short_id
            FROM
                ci_issue
                JOIN github_repository ON github_repository.id = ci_issue.repository_id


        """
        return sqlalchemy.type_coerce(
            sqlalchemy.func.concat(
                sqlalchemy.func.upper(gh_models.GitHubRepository.name),
                "-",
                cls.short_id_suffix,
            ),
            sqlalchemy.Text,
        ).label("short_id")


class CiIssue(models.Base, CiIssueMixin):
    __tablename__ = "ci_issue"

    __table_args__ = (
        sqlalchemy.UniqueConstraint(
            "repository_id",
            "short_id_suffix",
            name="ci_issue_repository_id_short_id_suffix_key",
        ),
    )

    jobs: orm.Mapped[list[gh_models.WorkflowJob]] = orm.relationship(
        "WorkflowJob",
        back_populates="ci_issue",
        lazy="raise_on_sql",
    )

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository_id: github_types.GitHubRepositoryIdType,
    ) -> CiIssue:
        issue = cls(
            repository_id=repository_id,
            short_id_suffix=await CiIssueCounter.get_next_val(session, repository_id),
            # NOTE(Kntrolix): we get the repository during insert to mimic the lazy join that whould have been done on select
            # and to be able to use this instance the same way as it was get from select
            repository=await session.get_one(gh_models.GitHubRepository, repository_id),
        )
        session.add(issue)
        return issue

    @classmethod
    async def link_job_to_ci_issue(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        job: gh_models.WorkflowJob,
    ) -> None:
        if job.log_embedding is None:
            raise RuntimeError("link_job_to_ci_issue called with a pending job")

        if job.ci_issue_id is not None:
            raise RuntimeError(
                "link_job_to_ci_issue called with a job already linked to an issue",
            )

        stmt = (
            sqlalchemy.select(gh_models.WorkflowJob.ci_issue_id)
            .where(
                gh_models.WorkflowJob.repository_id == job.repository_id,
                gh_models.WorkflowJob.name_without_matrix == job.name_without_matrix,
                gh_models.WorkflowJob.ci_issue_id.isnot(None),
                (
                    1
                    - (
                        gh_models.WorkflowJob.log_embedding.cosine_distance(
                            job.log_embedding,
                        )
                    )
                )
                >= COSINE_SIMILARITY_THRESHOLD,
            )
            .order_by(
                (
                    1
                    - (
                        gh_models.WorkflowJob.log_embedding.cosine_distance(
                            job.log_embedding,
                        )
                    )
                ).desc(),
            )
            .limit(1)
        )

        ci_issue_id = (await session.execute(stmt)).scalar_one_or_none()
        if ci_issue_id is None:
            job.ci_issue = await CiIssue.insert(session, job.repository_id)
        else:
            job.ci_issue_id = ci_issue_id


class PullRequestImpacted(typing.TypedDict):
    number: int
    title: str
    author: str
    events_count: int


class CiIssueGPT(models.Base, CiIssueMixin):
    __tablename__ = "ci_issue_gpt"

    __table_args__ = (
        sqlalchemy.UniqueConstraint(
            "repository_id",
            "short_id_suffix",
            name="ci_issue_gpt_repository_id_short_id_suffix_key",
        ),
    )

    jobs: orm.Mapped[list[gh_models.WorkflowJob]] = orm.relationship(
        "WorkflowJob",
        secondary="gha_workflow_job_log_metadata",
        lazy="raise_on_sql",
        back_populates="ci_issues_gpt",
        viewonly=True,
    )

    log_metadata: orm.Mapped[list[gh_models.WorkflowJobLogMetadata]] = orm.relationship(
        "WorkflowJobLogMetadata",
        lazy="raise_on_sql",
    )

    pull_requests_impacted: orm.Mapped[
        list[PullRequestImpacted] | None
    ] = orm.query_expression()

    @classmethod
    def with_pull_requests_impacted_column(cls) -> orm.strategy_options._AbstractLoad:
        distinct_subquery = (
            sqlalchemy.select(
                gh_pull_request.PullRequest.number,
                gh_pull_request.PullRequest.title,
                gh_models.GitHubAccount.login,
                sqlalchemy.func.count(
                    gh_models.WorkflowJobLogMetadata.id.distinct(),
                ).label("events"),
            )
            .select_from(gh_pull_request.PullRequest)
            .join(
                gh_models.WorkflowJobLogMetadata,
                gh_models.WorkflowJobLogMetadata.ci_issue_id == cls.id,
            )
            .join(
                gh_models.WorkflowJob,
                gh_models.WorkflowJob.id
                == gh_models.WorkflowJobLogMetadata.workflow_job_id,
            )
            .join(
                gh_models.GitHubAccount,
                gh_models.GitHubAccount.id == gh_pull_request.PullRequest.user_id,
            )
            .where(
                gh_pull_request.PullRequest.base_repository_id == cls.repository_id,
                gh_pull_request.PullRequest.head_sha_history.any(
                    gh_pull_request.PullRequestHeadShaHistory.head_sha
                    == gh_models.WorkflowJob.head_sha,
                ),
            )
            .group_by(gh_pull_request.PullRequest.id, gh_models.GitHubAccount.id)
            .order_by(
                sqlalchemy.desc("events"),
                gh_pull_request.PullRequest.number,
            )
            .correlate(CiIssueGPT)
            .subquery()
        )
        return orm.with_expression(
            cls.pull_requests_impacted,
            sqlalchemy.select(
                sqlalchemy.func.json_agg(
                    sqlalchemy.func.json_build_object(
                        "number",
                        distinct_subquery.c.number,
                        "title",
                        distinct_subquery.c.title,
                        "author",
                        distinct_subquery.c.login,
                        "events_count",
                        distinct_subquery.c.events,
                    ),
                ),
            ).scalar_subquery(),
        )

    @sqlalchemy.orm.declared_attr
    def events_count(cls) -> orm.Mapped[int]:
        return orm.deferred(
            sqlalchemy.select(sqlalchemy.func.count())
            .where(
                gh_models.WorkflowJobLogMetadata.ci_issue_id == cls.id,
            )
            .scalar_subquery(),
        )

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository_id: github_types.GitHubRepositoryIdType,
        log_metadata: gh_models.WorkflowJobLogMetadata,
    ) -> CiIssueGPT:
        issue = cls(
            repository_id=repository_id,
            short_id_suffix=await CiIssueCounter.get_next_val(session, repository_id),
            name=log_metadata.problem_type,
            log_metadata=[log_metadata],
        )
        session.add(issue)
        return issue

    @classmethod
    async def link_job_to_ci_issues(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        job: gh_models.WorkflowJob,
    ) -> None:
        if not job.log_metadata:
            raise RuntimeError("link_job_to_ci_issues called with a pending job")

        if job.ci_issues_gpt:
            raise RuntimeError(
                "link_job_to_ci_issues called with a job already linked to at least one issue",
            )

        for metadata in job.log_metadata:
            score_computing_expr = (
                sqlalchemy.case(
                    (
                        gh_models.WorkflowJobLogMetadata.problem_type
                        == metadata.problem_type,
                        1,
                    ),
                    else_=0,
                )
                + sqlalchemy.case(
                    (gh_models.WorkflowJobLogMetadata.language == metadata.language, 1),
                    else_=0,
                )
                + sqlalchemy.case(
                    (gh_models.WorkflowJobLogMetadata.filename == metadata.filename, 1),
                    else_=0,
                )
                + sqlalchemy.case(
                    (gh_models.WorkflowJobLogMetadata.lineno == metadata.lineno, 1),
                    else_=0,
                )
                + sqlalchemy.case(
                    (gh_models.WorkflowJobLogMetadata.error == metadata.error, 1),
                    else_=0,
                )
                + sqlalchemy.case(
                    (
                        gh_models.WorkflowJobLogMetadata.test_framework
                        == metadata.test_framework,
                        1,
                    ),
                    else_=0,
                )
                + sqlalchemy.case(
                    (
                        gh_models.WorkflowJobLogMetadata.stack_trace
                        == metadata.stack_trace,
                        6,
                    ),
                    else_=0,
                )
            )

            stmt = (
                sqlalchemy.select(cls, score_computing_expr)
                .options(orm.joinedload(cls.log_metadata))
                .join(
                    gh_models.WorkflowJobLogMetadata,
                    gh_models.WorkflowJobLogMetadata.ci_issue_id == cls.id,
                )
                .join(
                    gh_models.WorkflowJob,
                    gh_models.WorkflowJob.id
                    == gh_models.WorkflowJobLogMetadata.workflow_job_id,
                )
                .where(
                    gh_models.WorkflowJob.name_without_matrix
                    == job.name_without_matrix,
                    gh_models.WorkflowJob.repository_id == job.repository_id,
                    score_computing_expr >= MIN_MATCHING_SCORE,
                )
                .order_by(
                    score_computing_expr.desc(),
                )
                .limit(1)
            )

            issue = (await session.execute(stmt)).unique().scalar_one_or_none()

            if issue is None:
                issue = await CiIssueGPT.insert(
                    session,
                    job.repository_id,
                    metadata,
                )
            else:
                issue.log_metadata.append(metadata)
