from __future__ import annotations

import datetime  # noqa: TCH003
import enum
import re
import typing

import daiquiri
import numpy as np  # noqa: TCH002
import numpy.typing as npt  # noqa: TCH002
from pgvector.sqlalchemy import Vector  # type: ignore
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
import sqlalchemy.exc
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.hybrid

from mergify_engine import constants
from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.flaky_check.utils import NeedRerunStatus
from mergify_engine.models.github import account as gh_account
from mergify_engine.models.github import repository as gh_repository


if typing.TYPE_CHECKING:
    from mergify_engine.models.ci_issue import CiIssue

LOG = daiquiri.getLogger(__name__)

NAME_AND_MATRIX_RE = re.compile(r"^([\w|-]+) \((.+)\)$")


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
            if "triggering_actor" in workflow_run_data:
                triggering_actor = await gh_account.GitHubAccount.get_or_create(
                    session,
                    workflow_run_data["triggering_actor"],
                )
                session.add(triggering_actor)
            else:
                triggering_actor = None  # type: ignore[unreachable]

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


class WorkflowJobColumnMixin:
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

    @orm.declared_attr
    def repository(self) -> orm.Mapped[gh_repository.GitHubRepository]:
        return orm.relationship(lazy="joined")

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

    embedded_log: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.String,
        anonymizer_config="anon.lorem_ipsum( words := 20 )",
    )

    log_status: orm.Mapped[WorkflowJobLogStatus] = orm.mapped_column(
        sqlalchemy.Enum(WorkflowJobLogStatus),
        server_default=WorkflowJobLogStatus.UNKNOWN.name,
        anonymizer_config=None,
    )
    log_embedding_attempts: orm.Mapped[int] = orm.mapped_column(
        server_default="0",
        anonymizer_config=None,
    )
    log_embedding_retry_after: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        nullable=True,
        anonymizer_config=None,
    )
    matrix: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    head_sha: orm.Mapped[github_types.SHAType] = orm.mapped_column(
        sqlalchemy.String,
        anonymizer_config=None,
    )

    ci_issue_id: orm.Mapped[int | None] = orm.mapped_column(
        sqlalchemy.ForeignKey("ci_issue.id"),
        anonymizer_config=None,
        index=True,
    )

    @orm.declared_attr
    def ci_issue(self) -> orm.Mapped[CiIssue]:
        return orm.relationship(lazy="raise_on_sql")

    log_downloading_attempts: orm.Mapped[int] = orm.mapped_column(
        server_default="0",
        anonymizer_config=None,
    )
    log_downloading_retry_after: orm.Mapped[
        datetime.datetime | None
    ] = orm.mapped_column(
        nullable=True,
        anonymizer_config=None,
    )

    log_embedding_status: orm.Mapped[WorkflowJobLogEmbeddingStatus] = orm.mapped_column(
        sqlalchemy.Enum(WorkflowJobLogEmbeddingStatus),
        server_default=WorkflowJobLogEmbeddingStatus.UNKNOWN.name,
        anonymizer_config=None,
    )
    log_metadata_extracting_attempts: orm.Mapped[int] = orm.mapped_column(
        server_default="0",
        anonymizer_config=None,
    )
    log_metadata_extracting_retry_after: orm.Mapped[
        datetime.datetime | None
    ] = orm.mapped_column(
        nullable=True,
        anonymizer_config=None,
    )
    log_metadata_extracting_status: orm.Mapped[
        WorkflowJobLogMetadataExtractingStatus
    ] = orm.mapped_column(
        sqlalchemy.Enum(WorkflowJobLogMetadataExtractingStatus),
        server_default=WorkflowJobLogMetadataExtractingStatus.UNKNOWN.name,
        anonymizer_config=None,
    )


class WorkflowJob(models.Base, WorkflowJobColumnMixin):
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
            (
                log_embedding_status = 'EMBEDDED' AND log_embedding IS NOT NULL
            ) OR (
                log_embedding_status != 'EMBEDDED' AND log_embedding IS NULL
            )
            """,
            name="embedding_linked_columns",
        ),
        sqlalchemy.schema.CheckConstraint(
            """
            log_embedding_attempts >= 0 AND (
                ( log_embedding_attempts = 0 AND log_embedding_retry_after IS NULL )
                OR ( log_embedding_attempts > 0 AND log_embedding_retry_after IS NOT NULL)
            )
            """,
            name="embedding_retries",
        ),
        sqlalchemy.schema.CheckConstraint(
            """
            log_metadata_extracting_attempts >= 0 AND (
                ( log_metadata_extracting_attempts = 0 AND log_metadata_extracting_retry_after IS NULL )
                OR ( log_metadata_extracting_attempts > 0 AND log_metadata_extracting_retry_after IS NOT NULL)
            )
            """,
            name="metadata_extracting_retries",
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

    log_metadata: orm.Mapped[list[WorkflowJobLogMetadata]] = orm.relationship(
        "WorkflowJobLogMetadata",
        back_populates="workflow_job",
        lazy="raise_on_sql",
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
                "log_embedding_attempts": self.log_embedding_attempts,
                "log_embedding_retry_after": self.log_embedding_retry_after,
                "log_metadata_extracting_status": self.log_metadata_extracting_status,
                "log_metadata_extracting_attempts": self.log_metadata_extracting_attempts,
                "log_metadata_extracting_retry_after": self.log_metadata_extracting_retry_after,
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
        # Avoid circular import
        from mergify_engine.models.views.workflows import FlakyStatus
        from mergify_engine.models.views.workflows import WorkflowJobEnhanced

        stmt = sqlalchemy.select(
            cls.run_attempt,
            cls.conclusion,
            cls.ci_issue_id,
            sqlalchemy.func.exists(
                sqlalchemy.select(sqlalchemy.literal_column("1", sqlalchemy.INT))
                .select_from(WorkflowJobEnhanced)
                .where(
                    WorkflowJobEnhanced.ci_issue_id == cls.ci_issue_id,
                    WorkflowJobEnhanced.repository_id == cls.repository_id,
                    WorkflowJobEnhanced.flaky == FlakyStatus.FLAKY.value,
                )
                .scalar_subquery(),
            ).label("flaky_neighb"),
        ).where(cls.id == job_id)

        try:
            result = (await session.execute(stmt)).one()
        except sqlalchemy.exc.NoResultFound:
            # NOTE(Kontrolix): We haven't yet received the job
            return NeedRerunStatus.UNKONWN

        if result.ci_issue_id is None:
            # NOTE(Kontrolix): Safety in case we would have called this method with a
            # succesful job. Normaly we should never enter this.
            if result.conclusion == WorkflowJobConclusion.SUCCESS:
                return NeedRerunStatus.DONT_NEED_RERUN

            # NOTE(Kontrolix): Job is not yet linked to a ci issue
            return NeedRerunStatus.UNKONWN

        # NOTE(Kontrolix): We use the run_attempt value to know how many time
        # this job has been rerun. It's imperfect because if a human rerun manually
        # the job it will be taken into account as if Mergify already rerun it

        # NOTE(Konrolix): Case where there is a known flaky job as neighbour
        if result.flaky_neighb is True and result.run_attempt < max_rerun:
            return NeedRerunStatus.NEED_RERUN

        # NOTE(Kontrolix): Case where there is no known flaky neighbour so we
        # rerun once to try
        if not result.flaky_neighb and result.run_attempt == 1:
            return NeedRerunStatus.NEED_RERUN

        return NeedRerunStatus.DONT_NEED_RERUN

    def as_github_dict(self) -> GitHubWorkflowJobDict:
        return typing.cast(GitHubWorkflowJobDict, super().as_github_dict())


class WorkflowJobLogMetadata(models.Base):
    __tablename__ = "gha_workflow_job_log_metadata"

    id: orm.Mapped[int] = orm.mapped_column(
        primary_key=True,
        autoincrement=True,
        anonymizer_config=None,
    )

    workflow_job_id: orm.Mapped[WorkflowJob] = orm.mapped_column(
        sqlalchemy.ForeignKey("gha_workflow_job.id"),
        anonymizer_config=None,
        index=True,
    )

    workflow_job: orm.Mapped[WorkflowJob] = orm.relationship(
        lazy="joined",
    )

    problem_type: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    language: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    filename: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    lineno: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    error: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    test_framework: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)

    stack_trace: orm.Mapped[str | None] = orm.mapped_column(anonymizer_config=None)
