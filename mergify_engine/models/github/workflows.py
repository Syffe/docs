from __future__ import annotations

import dataclasses
import datetime  # noqa: TCH003
import enum
import re
import typing

import daiquiri
import numpy as np  # noqa: TCH002
import numpy.typing as npt  # noqa: TCH002
from pgvector.sqlalchemy import Vector  # type: ignore[import-untyped]
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
import sqlalchemy.exc
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.hybrid

from mergify_engine import constants
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.flaky_check.utils import NeedRerunStatus
from mergify_engine.models.github import account as gh_account
from mergify_engine.models.github import pull_request as gh_pull_request
from mergify_engine.models.github import repository as gh_repository


if typing.TYPE_CHECKING:
    from mergify_engine.models import ci_issue as ci_issue_model

LOG = daiquiri.getLogger(__name__)

NAME_AND_MATRIX_RE = re.compile(r"^([\w|-]+) \((.+)\)$")


FlakyStatusT = typing.Literal["flaky", "unknown"]


class GitHubWorkflowJobDict(typing.TypedDict):
    id: int
    workflow_run_id: int
    github_name: str
    started_at: github_types.ISODateTimeType
    completed_at: github_types.ISODateTimeType
    conclusion: WorkflowJobConclusion
    labels: list[str]
    repository_id: github_types.GitHubRepositoryIdType
    repository: github_types.GitHubRepository
    run_attempt: int
    steps: list[github_types.GitHubWorkflowJobStep]
    head_sha: github_types.SHAType


class WorkflowJobConclusion(enum.Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"
    NEUTRAL = "neutral"
    TIMED_OUT = "timed_out"
    ACTION_REQUIRED = "action_required"


class WorkflowRunTriggerEvent(enum.Enum):
    PULL_REQUEST = "pull_request"
    PULL_REQUEST_TARGET = "pull_request_target"
    PUSH = "push"
    SCHEDULE = "schedule"


class WorkflowRun(models.Base):
    __tablename__ = "gha_workflow_run"
    __table_args__ = (
        sqlalchemy.Index(
            "gha_workflow_run_owner_id_repository_id_idx",
            "owner_id",
            "repository_id",
        ),
    )
    __repr_attributes__: typing.ClassVar[tuple[str, ...]] = ("id",)

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=False,
        anonymizer_config=None,
    )
    workflow_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        anonymizer_config="anon.random_bigint_between(1,100000)",
    )
    owner_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id"),
        anonymizer_config=None,
    )
    event: orm.Mapped[WorkflowRunTriggerEvent] = orm.mapped_column(
        sqlalchemy.Enum(WorkflowRunTriggerEvent),
        anonymizer_config="anon.random_in_enum(event)",
    )
    triggering_actor_id: orm.Mapped[int | None] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id"),
        anonymizer_config=None,
    )
    run_attempt: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        anonymizer_config=None,
    )

    owner: orm.Mapped[gh_account.GitHubAccount] = orm.relationship(
        lazy="joined",
        foreign_keys=[owner_id],
    )
    triggering_actor: orm.Mapped[gh_account.GitHubAccount] = orm.relationship(
        lazy="joined",
        foreign_keys=[triggering_actor_id],
    )

    repository_id: orm.Mapped[github_types.GitHubRepositoryIdType] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_repository.id"),
        anonymizer_config=None,
    )

    repository: orm.Mapped[gh_repository.GitHubRepository] = orm.relationship(
        lazy="joined",
    )

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        workflow_run_data: github_types.GitHubWorkflowRun,
        repository: github_types.GitHubRepository,
    ) -> None:
        result = await session.execute(
            sqlalchemy.select(cls).where(cls.id == workflow_run_data["id"]),
        )
        if result.scalar_one_or_none() is None:
            if (
                triggering_actor_data := workflow_run_data.get("triggering_actor")
            ) is not None:
                triggering_actor = await gh_account.GitHubAccount.get_or_create(
                    session,
                    triggering_actor_data,
                )
                session.add(triggering_actor)
            else:
                triggering_actor = None

            repo = await gh_repository.GitHubRepository.get_or_create(
                session,
                repository,
            )

            session.add(
                cls(
                    id=workflow_run_data["id"],
                    workflow_id=workflow_run_data["workflow_id"],
                    owner=repo.owner,
                    repository=repo,
                    event=WorkflowRunTriggerEvent(workflow_run_data["event"]),
                    triggering_actor=triggering_actor,
                    run_attempt=workflow_run_data["run_attempt"],
                ),
            )


class WorkflowJobLogStatus(enum.Enum):
    UNKNOWN = "unknown"
    GONE = "gone"
    ERROR = "error"
    DOWNLOADED = "downloaded"


class WorkflowJobLogEmbeddingStatus(enum.Enum):
    UNKNOWN = "unknown"
    ERROR = "error"
    EMBEDDED = "embedded"


class WorkflowJobLogMetadataExtractingStatus(enum.Enum):
    UNKNOWN = "unknown"
    ERROR = "error"
    EXTRACTED = "extracted"


class WorkflowJobFailedStep(typing.TypedDict):
    number: int
    name: str


class WorkflowJob(models.Base):
    __tablename__ = "gha_workflow_job"

    __table_args__ = (
        sqlalchemy.schema.Index(
            "idx_gha_workflow_job_conclusion_failure_repository_id",
            "conclusion",
            "repository_id",
            postgresql_where="conclusion = 'FAILURE'::workflowjobconclusion",
        ),
        sqlalchemy.schema.Index(
            "idx_gha_workflow_job_rerun_compound",
            "run_attempt",
            "repository_id",
            "name_without_matrix",
            "workflow_run_id",
            "conclusion",
            postgresql_where="conclusion = 'SUCCESS'::workflowjobconclusion",
        ),
        sqlalchemy.schema.Index(
            # NOTE(sileht): Index used for the big select used by monitoring and by log_embedder.github_action.get_embeds()
            "idx_gha_workflow_job_incomplete_job_finder",
            "conclusion",
            "log_status",
            "failed_step_number",
            postgresql_where="conclusion = 'FAILURE'::workflowjobconclusion AND failed_step_number IS NOT NULL AND log_status <> ALL (ARRAY['GONE'::workflowjoblogstatus, 'ERROR'::workflowjoblogstatus])",
        ),
        sqlalchemy.schema.Index(
            "idx_gha_workflow_job_same_job_name_in_repo",
            "name_without_matrix",
            "repository_id",
        ),
        sqlalchemy.schema.Index(
            "idx_gha_workflow_job_ci_issue_id_in_repo",
            "repository_id",
            "ci_issue_id",
        ),
        sqlalchemy.schema.CheckConstraint(
            """
            log_processing_attempts >= 0 AND (
                ( log_processing_attempts = 0 AND log_processing_retry_after IS NULL )
                OR ( log_processing_attempts > 0 AND log_processing_retry_after IS NOT NULL)
            )
            """,
            name="processing_retries",
        ),
    )

    __repr_attributes__: typing.ClassVar[tuple[str, ...]] = ("id",)
    __github_attributes__ = (
        "id",
        "workflow_run_id",
        "github_name",
        "started_at",
        "completed_at",
        "conclusion",
        "labels",
        "repository_id",
        "repository",
        "run_attempt",
        "steps",
        "head_sha",
    )

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=False,
        anonymizer_config=None,
    )
    workflow_run_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        anonymizer_config=None,
    )
    name_without_matrix: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.String,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    started_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        anonymizer_config="anon.dnoise(started_at, ''1 hour'')",
    )
    completed_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        anonymizer_config="anon.dnoise(completed_at, ''1 hour'')",
        index=True,
    )
    conclusion: orm.Mapped[WorkflowJobConclusion] = orm.mapped_column(
        sqlalchemy.Enum(WorkflowJobConclusion),
        anonymizer_config="anon.random_in_enum(conclusion)",
    )
    labels: orm.Mapped[list[str]] = orm.mapped_column(
        postgresql.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(0, 5, 20)",
    )

    log_embedding: orm.Mapped[npt.NDArray[np.float32] | None] = orm.mapped_column(
        Vector(constants.OPENAI_EMBEDDING_DIMENSION),
        nullable=True,
        anonymizer_config=None,
    )

    repository_id: orm.Mapped[github_types.GitHubRepositoryIdType] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_repository.id"),
        anonymizer_config=None,
    )

    repository: orm.Mapped[gh_repository.GitHubRepository] = orm.relationship(
        lazy="joined",
    )

    run_attempt: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        anonymizer_config=None,
    )

    steps: orm.Mapped[
        list[github_types.GitHubWorkflowJobStep] | None
    ] = orm.mapped_column(sqlalchemy.JSON, anonymizer_config=None, nullable=True)

    failed_step_number: orm.Mapped[int | None] = orm.mapped_column(
        sqlalchemy.Integer,
        anonymizer_config=None,
    )
    failed_step_name: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.String,
        anonymizer_config=None,
    )

    log_extract: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.String,
        anonymizer_config="anon.lorem_ipsum( words := 20 )",
    )

    log_status: orm.Mapped[WorkflowJobLogStatus] = orm.mapped_column(
        sqlalchemy.Enum(WorkflowJobLogStatus),
        server_default=WorkflowJobLogStatus.UNKNOWN.name,
        anonymizer_config=None,
    )
    matrix: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    head_sha: orm.Mapped[github_types.SHAType] = orm.mapped_column(
        sqlalchemy.String,
        anonymizer_config=None,
        index=True,
    )

    ci_issue_id: orm.Mapped[int | None] = orm.mapped_column(
        sqlalchemy.ForeignKey("ci_issue.id"),
        anonymizer_config=None,
        index=True,
    )

    ci_issue: orm.Mapped[ci_issue_model.CiIssue] = orm.relationship(
        "CiIssue",
        lazy="raise_on_sql",
    )

    log_embedding_status: orm.Mapped[WorkflowJobLogEmbeddingStatus] = orm.mapped_column(
        sqlalchemy.Enum(WorkflowJobLogEmbeddingStatus),
        server_default=WorkflowJobLogEmbeddingStatus.UNKNOWN.name,
        anonymizer_config=None,
    )
    log_metadata_extracting_status: orm.Mapped[
        WorkflowJobLogMetadataExtractingStatus
    ] = orm.mapped_column(
        sqlalchemy.Enum(WorkflowJobLogMetadataExtractingStatus),
        server_default=WorkflowJobLogMetadataExtractingStatus.UNKNOWN.name,
        anonymizer_config=None,
    )

    log_processing_attempts: orm.Mapped[int] = orm.mapped_column(
        server_default="0",
        anonymizer_config=None,
    )
    log_processing_retry_after: orm.Mapped[
        datetime.datetime | None
    ] = orm.mapped_column(
        nullable=True,
        anonymizer_config=None,
    )

    log_metadata: orm.Mapped[list[WorkflowJobLogMetadata]] = orm.relationship(
        "WorkflowJobLogMetadata",
        back_populates="workflow_job",
        lazy="raise_on_sql",
    )

    ci_issues_gpt: orm.Mapped[list[ci_issue_model.CiIssueGPT]] = orm.relationship(
        "CiIssueGPT",
        lazy="raise_on_sql",
        back_populates="jobs",
        secondary="gha_workflow_job_log_metadata",
        viewonly=True,
    )

    @sqlalchemy.ext.hybrid.hybrid_property
    def github_name(self) -> str:
        if self.matrix is not None:
            return f"{self.name_without_matrix} ({self.matrix})"
        return self.name_without_matrix

    @github_name.inplace.expression
    @classmethod
    def _github_name_expression(cls) -> sqlalchemy.ColumnElement[str]:
        return sqlalchemy.case(
            (
                cls.matrix.isnot(None),
                sqlalchemy.func.concat(cls.name_without_matrix, " (", cls.matrix, ")"),
            ),
            else_=cls.name_without_matrix,
        )

    failed_run_count: orm.Mapped[int] = orm.query_expression()

    @classmethod
    def with_failed_run_count_column(cls) -> orm.strategy_options._AbstractLoad:
        # NOTE(sileht): we can't use column_property due to self referencing GitHubWorkflowJob
        WorkflowJobSibling = orm.aliased(
            WorkflowJob,
            name="workflow_job_sibling_failed_run_count",
        )
        return orm.with_expression(
            cls.failed_run_count,
            sqlalchemy.select(sqlalchemy.func.count())
            .where(
                WorkflowJobSibling.repository_id == cls.repository_id,
                WorkflowJobSibling.workflow_run_id == cls.workflow_run_id,
                WorkflowJobSibling.name_without_matrix == cls.name_without_matrix,
                sqlalchemy.or_(
                    sqlalchemy.and_(
                        WorkflowJobSibling.matrix.is_(None),
                        cls.matrix.is_(None),
                    ),
                    WorkflowJobSibling.matrix == cls.matrix,
                ),
                WorkflowJobSibling.conclusion == WorkflowJobConclusion.FAILURE,
            )
            .scalar_subquery(),
        )

    pull_requests: orm.Mapped[
        list[github_types.GitHubPullRequestNumber]
    ] = orm.query_expression()

    @classmethod
    def with_pull_requests_column(cls) -> orm.strategy_options._AbstractLoad:
        return orm.with_expression(
            cls.pull_requests,
            sqlalchemy.select(
                sqlalchemy.func.array_agg(
                    gh_pull_request.PullRequest.number.distinct(),
                ),
            )
            .where(
                gh_pull_request.PullRequest.base_repository_id == cls.repository_id,
                gh_pull_request.PullRequest.head_sha_history.any(
                    gh_pull_request.PullRequestHeadShaHistory.head_sha == cls.head_sha,
                ),
            )
            .group_by(gh_pull_request.PullRequest.base_repository_id)
            .correlate(WorkflowJob)
            .scalar_subquery(),
        )

    flaky: orm.Mapped[FlakyStatusT] = orm.query_expression()

    @classmethod
    def with_flaky_column(cls) -> orm.strategy_options._AbstractLoad:
        # NOTE(sileht): we can't use column_property due to self referencing GitHubWorkflowJob
        WorkflowJobSibling = orm.aliased(WorkflowJob, name="workflow_job_sibling_flaky")
        return orm.with_expression(
            cls.flaky,
            sqlalchemy.select(
                sqlalchemy.case(
                    (sqlalchemy.func.count() >= 1, "flaky"),
                    else_="unknown",
                ),
            )
            .where(
                WorkflowJobSibling.conclusion == WorkflowJobConclusion.SUCCESS,
                WorkflowJobSibling.repository_id == cls.repository_id,
                WorkflowJobSibling.workflow_run_id == cls.workflow_run_id,
                WorkflowJobSibling.name_without_matrix == cls.name_without_matrix,
                sqlalchemy.or_(
                    sqlalchemy.and_(
                        WorkflowJobSibling.matrix.is_(None),
                        cls.matrix.is_(None),
                    ),
                    WorkflowJobSibling.matrix == cls.matrix,
                ),
            )
            .scalar_subquery(),
        )

    def as_log_extras(self) -> dict[str, typing.Any]:
        return {
            "gh_owner": self.repository.owner.login,
            "gh_repo": self.repository.name,
            "job": {
                "id": self.id,
                "workflow_run_id": self.workflow_run_id,
                "name_without_matrix": self.name_without_matrix,
                "conclusion": self.conclusion,
                "steps": self.steps,
                "run_attempt": self.run_attempt,
                "started_at": self.started_at,
                "completed_at": self.completed_at,
                "log_status": self.log_status,
                "log_embedding_status": self.log_embedding_status,
                "log_metadata_extracting_status": self.log_metadata_extracting_status,
                "log_processing_attempts": self.log_processing_attempts,
                "log_processing_retry_after": self.log_processing_retry_after,
            },
        }

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        workflow_job_data: github_types.GitHubWorkflowJob,
        repository: github_types.GitHubRepository,
    ) -> WorkflowJob:
        result = await session.execute(
            sqlalchemy.select(cls).where(cls.id == workflow_job_data["id"]),
        )
        job = result.scalar_one_or_none()
        if job is None:
            failed_step = cls.get_failed_step(workflow_job_data)

            name_without_matrix, matrix = cls.get_job_name_and_matrix(
                workflow_job_data["name"],
            )
            repo = await gh_repository.GitHubRepository.get_or_create(
                session,
                repository,
            )
            job = cls(
                id=workflow_job_data["id"],
                workflow_run_id=workflow_job_data["run_id"],
                name_without_matrix=name_without_matrix,
                matrix=matrix,
                started_at=workflow_job_data["started_at"],
                completed_at=workflow_job_data["completed_at"],
                conclusion=WorkflowJobConclusion(workflow_job_data["conclusion"]),
                labels=workflow_job_data["labels"],
                repository=repo,
                repository_id=repo.id,
                run_attempt=workflow_job_data["run_attempt"],
                steps=workflow_job_data["steps"],
                failed_step_number=failed_step["number"] if failed_step else None,
                failed_step_name=failed_step["name"] if failed_step else None,
                # FIXME(sileht): future events will  head_sha events always set
                head_sha=workflow_job_data.get("head_sha", ""),
                ci_issues_gpt=[],
            )
            session.add(job)

        return job

    @staticmethod
    def get_job_name_and_matrix(name: str) -> tuple[str, str | None]:
        search_result = NAME_AND_MATRIX_RE.search(name)
        if search_result is not None:
            return search_result.groups()[0], search_result.groups()[1]
        return name, None

    @staticmethod
    def get_failed_step(
        workflow_job_data: github_types.GitHubWorkflowJob,
    ) -> WorkflowJobFailedStep | None:
        if workflow_job_data["conclusion"] != "failure":
            return None

        if not workflow_job_data["steps"]:
            return None

        for step in workflow_job_data["steps"]:
            if step["conclusion"] in ("failure", "cancelled"):
                return WorkflowJobFailedStep(number=step["number"], name=step["name"])

            if step["status"] != "completed":
                # steps was still in progress, we assume the job timed out
                # See MRGFY-2588
                return None

        return None

    @classmethod
    async def is_rerun_needed(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        job_id: int,
        max_rerun: int,
    ) -> NeedRerunStatus:
        stmt_job = (
            sqlalchemy.select(cls)
            .options(sqlalchemy.orm.load_only(cls.id, cls.conclusion, cls.run_attempt))
            .where(
                cls.id == job_id,
            )
        )
        job = (await session.execute(stmt_job)).scalar_one_or_none()
        if job is None:
            # NOTE(Kontrolix): We haven't yet received the job
            return NeedRerunStatus.UNKONWN

        if job.conclusion == WorkflowJobConclusion.SUCCESS:
            # NOTE(Kontrolix): Safety in case we would have called this method with a
            # succesful job. Normaly we should never enter this.
            # FIXME(sileht): This is not supposed to happen, but we have tests that assert this behavior...
            # We should: raise RuntimeError("is_rerun_needed called on a successful job")
            return NeedRerunStatus.DONT_NEED_RERUN

        if job.ci_issue_id is None:
            # NOTE(sileht): Not yet associated to a CI issue
            return NeedRerunStatus.UNKONWN

        # Circular import
        from mergify_engine.models import ci_issue as ci_issue_model

        # FIXME(sileht): Use CiIssueGPT here
        # The SQL request is far from been ideal as we list all jobs but only require one to mark issue as flaky
        # Since is use the old CiIssue table, and an unused feature, we don't really care for now.
        stmt_issue = (
            sqlalchemy.select(ci_issue_model.CiIssue)
            .options(
                sqlalchemy.orm.load_only(ci_issue_model.CiIssue.id),
                sqlalchemy.orm.joinedload(ci_issue_model.CiIssue.jobs)
                .load_only(cls.id)
                .options(cls.with_flaky_column()),
            )
            .where(
                ci_issue_model.CiIssue.id == job.ci_issue_id,
            )
        )

        issue = (await session.execute(stmt_issue)).unique().scalar_one_or_none()
        if issue is None:
            # CiIssue vanished since last SQL request
            return NeedRerunStatus.UNKONWN

        issue_is_flaky = any(job.flaky == "flaky" for job in issue.jobs)

        # NOTE(Kontrolix): We use the run_attempt value to know how many time
        # this job has been rerun. It's imperfect because if a human rerun manually
        # the job it will be taken into account as if Mergify already rerun it

        # NOTE(Konrolix): Case where there is a known flaky job as neighbour
        if issue_is_flaky and job.run_attempt < max_rerun:
            return NeedRerunStatus.NEED_RERUN

        # NOTE(Kontrolix): Case where there is no known flaky neighbour so we
        # rerun once to try
        # NOTE(sileht): Why are we doing this? This does not make sense to me, it should
        # be user choice, not ours.
        if not issue_is_flaky and job.run_attempt == 1:
            return NeedRerunStatus.NEED_RERUN

        return NeedRerunStatus.DONT_NEED_RERUN

    def as_github_dict(self) -> GitHubWorkflowJobDict:
        return typing.cast(GitHubWorkflowJobDict, super().as_github_dict())

    @classmethod
    async def delete_outdated(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        retention_time: datetime.timedelta,
    ) -> None:
        await session.execute(
            sqlalchemy.delete(cls).where(
                (date.utcnow() - cls.completed_at) > retention_time,
            ),
        )

    @dataclasses.dataclass
    class UnableToRetrieveLog(Exception):
        job: WorkflowJob

    async def download_failure_annotations(
        self,
        client: github.AsyncGitHubClient,
    ) -> list[str]:
        try:
            resp = await client.get(
                f"/repos/{self.repository.owner.login}/{self.repository.name}/check-runs/{self.id}/annotations",
            )
        except http.HTTPStatusError as e:
            if e.response.status_code in (410, 404):
                raise self.UnableToRetrieveLog(self)
            raise

        return [
            annotation["message"]
            for annotation in typing.cast(
                list[github_types.GitHubAnnotation],
                resp.json(),
            )
            if annotation["annotation_level"] == "failure"
        ]

    async def download_failed_logs(
        self,
        client: github.AsyncGitHubClient,
    ) -> bytes:
        try:
            resp = await client.get(
                f"/repos/{self.repository.owner.login}/{self.repository.name}/actions/runs/{self.workflow_run_id}/attempts/{self.run_attempt}/logs",
            )
        except http.HTTPStatusError as e:
            if e.response.status_code in (410, 404):
                raise self.UnableToRetrieveLog(self)
            raise

        return resp.content


class WorkflowJobLogMetadata(models.Base):
    __tablename__ = "gha_workflow_job_log_metadata"

    id: orm.Mapped[int] = orm.mapped_column(
        primary_key=True,
        autoincrement=True,
        anonymizer_config=None,
    )

    workflow_job_id: orm.Mapped[WorkflowJob] = orm.mapped_column(
        sqlalchemy.ForeignKey("gha_workflow_job.id", ondelete="CASCADE"),
        anonymizer_config=None,
        index=True,
    )

    workflow_job: orm.Mapped[WorkflowJob] = orm.relationship(
        "WorkflowJob",
        back_populates="log_metadata",
        lazy="joined",
    )

    problem_type: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    language: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    filename: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    lineno: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    error: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    test_framework: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    stack_trace: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    ci_issue_id: orm.Mapped[int | None] = orm.mapped_column(
        sqlalchemy.ForeignKey("ci_issue_gpt.id"),
        anonymizer_config=None,
    )

    test_name: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)
