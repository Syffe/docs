from __future__ import annotations

import datetime
import enum
import typing

import daiquiri
import numpy as np
import numpy.typing as npt
from pgvector.sqlalchemy import Vector  # type: ignore
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
import sqlalchemy.ext.asyncio

from mergify_engine import constants
from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.ci import pull_registries
from mergify_engine.models import github_account
from mergify_engine.models import github_repository


LOG = daiquiri.getLogger(__name__)


class PullRequest(models.Base):
    __tablename__ = "pull_request"

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=False,
        anonymizer_config=None,
    )
    number: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        anonymizer_config="anon.random_int_between(1,100000)",
    )
    title: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 5 )",
    )
    state: orm.Mapped[github_types.GitHubPullRequestState] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )

    workflow_runs: orm.Mapped[list[WorkflowRun]] = orm.relationship(
        secondary="jt_gha_workflow_run_pull_request",
        back_populates="pull_requests",
        viewonly=True,
    )

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        pull: pull_registries.PullRequest,
    ) -> None:
        sql = (
            postgresql.insert(cls)
            .values(
                id=pull.id,
                number=pull.number,
                title=pull.title,
                state=pull.state,
            )
            .on_conflict_do_update(
                index_elements=[cls.id],
                set_={"number": pull.number, "title": pull.title},
            )
        )
        await session.execute(sql)


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
        sqlalchemy.BigInteger, anonymizer_config=None
    )

    owner: orm.Mapped[github_account.GitHubAccount] = orm.relationship(
        lazy="joined", foreign_keys=[owner_id]
    )
    triggering_actor: orm.Mapped[github_account.GitHubAccount] = orm.relationship(
        lazy="joined", foreign_keys=[triggering_actor_id]
    )
    pull_requests: orm.Mapped[list[PullRequest]] = orm.relationship(
        secondary="jt_gha_workflow_run_pull_request",
        back_populates="workflow_runs",
        viewonly=True,
        lazy="selectin",
    )

    repository_id: orm.Mapped[github_types.GitHubRepositoryIdType] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_repository.id"),
        anonymizer_config=None,
    )

    repository: orm.Mapped[github_repository.GitHubRepository] = orm.relationship(
        lazy="joined"
    )

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        workflow_run_data: github_types.GitHubWorkflowRun,
        repository: github_types.GitHubRepository,
    ) -> None:
        result = await session.execute(
            sqlalchemy.select(cls).where(cls.id == workflow_run_data["id"])
        )
        if result.scalar_one_or_none() is None:
            if "triggering_actor" in workflow_run_data:
                triggering_actor = await github_account.GitHubAccount.get_or_create(
                    session, workflow_run_data["triggering_actor"]
                )
                session.add(triggering_actor)
            else:
                triggering_actor = None  # type: ignore[unreachable]

            repo = await github_repository.GitHubRepository.get_or_create(
                session, repository
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
                )
            )


class WorkflowJobLogStatus(enum.Enum):
    UNKNOWN = "unknown"
    GONE = "gone"
    ERROR = "error"
    EMBEDDED = "embedded"


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
            "name",
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
        sqlalchemy.schema.CheckConstraint(
            """
            (
                log_status = 'EMBEDDED' AND log_embedding IS NOT NULL AND embedded_log IS NOT NULL
            ) OR (
                log_status != 'EMBEDDED' AND log_embedding IS NULL AND embedded_log IS NULL
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
    name: orm.Mapped[str] = orm.mapped_column(
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
        sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1),
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

    repository: orm.Mapped[github_repository.GitHubRepository] = orm.relationship(
        lazy="joined"
    )

    neighbours_computed_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        nullable=True, anonymizer_config=None
    )

    run_attempt: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger, anonymizer_config=None
    )

    steps: orm.Mapped[
        list[github_types.GitHubWorkflowJobStep] | None
    ] = orm.mapped_column(sqlalchemy.JSON, anonymizer_config=None, nullable=True)

    failed_step_number: orm.Mapped[int | None] = orm.mapped_column(
        sqlalchemy.Integer, anonymizer_config=None
    )
    failed_step_name: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.String, anonymizer_config=None
    )

    embedded_log: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.String,
        anonymizer_config="anon.lorem_ipsum( words := 20 )",
    )

    embedded_log_error_title: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.String,
        anonymizer_config="anon.lorem_ipsum( words := 10 )",
    )
    log_status: orm.Mapped[WorkflowJobLogStatus] = orm.mapped_column(
        sqlalchemy.Enum(WorkflowJobLogStatus),
        server_default="UNKNOWN",
        anonymizer_config=None,
    )
    log_embedding_attempts: orm.Mapped[int] = orm.mapped_column(
        server_default="0", anonymizer_config=None
    )
    log_embedding_retry_after: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        nullable=True, anonymizer_config=None
    )

    def as_log_extras(self) -> dict[str, typing.Any]:
        return {
            "gh_owner": self.repository.owner.login,
            "gh_repo": self.repository.name,
            "job": {
                "id": self.id,
                "workflow_run_id": self.workflow_run_id,
                "name": self.name,
                "conclusion": self.conclusion,
                "steps": self.steps,
                "run_attempt": self.run_attempt,
                "started_at": self.started_at,
                "completed_at": self.completed_at,
                "log_status": self.log_status,
                "log_embedding_attempts": self.log_embedding_attempts,
                "log_embedding_retry_after": self.log_embedding_retry_after,
                "neighbours_computed_at": self.neighbours_computed_at,
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
            sqlalchemy.select(cls).where(cls.id == workflow_job_data["id"])
        )
        job = result.scalar_one_or_none()
        if job is None:
            failed_step = cls.get_failed_step(workflow_job_data, repository)

            job = cls(
                id=workflow_job_data["id"],
                workflow_run_id=workflow_job_data["run_id"],
                name=workflow_job_data["name"],
                started_at=workflow_job_data["started_at"],
                completed_at=workflow_job_data["completed_at"],
                conclusion=WorkflowJobConclusion(workflow_job_data["conclusion"]),
                labels=workflow_job_data["labels"],
                repository=await github_repository.GitHubRepository.get_or_create(
                    session, repository
                ),
                run_attempt=workflow_job_data["run_attempt"],
                steps=workflow_job_data["steps"],
                failed_step_number=failed_step["number"] if failed_step else None,
                failed_step_name=failed_step["name"] if failed_step else None,
            )
            session.add(job)

        return job

    @staticmethod
    def get_failed_step(
        workflow_job_data: github_types.GitHubWorkflowJob,
        repository: github_types.GitHubRepository,
    ) -> WorkflowJobFailedStep | None:
        # NOTE(Kontrolix): repository is passed as a parameter just for debug purpose
        # in DataDog.
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

        LOG.error(
            "WorkflowJob: Can't find failed step on failed job",
            workflow_job_data=workflow_job_data,
            gh_owner=repository["owner"]["login"],
            gh_repo=repository["name"],
        )
        return None

    @classmethod
    async def compute_logs_embedding_cosine_similarity(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        job_ids: list[int],
    ) -> None:
        if not job_ids:
            return

        job = orm.aliased(cls, name="job")
        other_job = orm.aliased(cls, name="other_job")

        select_statement = (
            sqlalchemy.select(
                job.id.label("job_id"),
                other_job.id.label("neighbour_job_id"),
                1
                - (job.log_embedding.cosine_distance(other_job.log_embedding)).label(
                    "cosine_similarity"
                ),
            )
            .select_from(job)
            .join(
                other_job,
                sqlalchemy.and_(
                    other_job.id != job.id,
                    other_job.name == job.name,
                    other_job.log_embedding.isnot(None),
                    other_job.repository_id == job.repository_id,
                ),
            )
            .where(job.id.in_(job_ids))
        )

        insert_statement = postgresql.insert(WorkflowJobLogNeighbours).from_select(
            [
                WorkflowJobLogNeighbours.job_id,
                WorkflowJobLogNeighbours.neighbour_job_id,
                WorkflowJobLogNeighbours.cosine_similarity,
            ],
            select_statement,
        )

        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=[
                WorkflowJobLogNeighbours.job_id,
                WorkflowJobLogNeighbours.neighbour_job_id,
            ],
            set_={"cosine_similarity": insert_statement.excluded.cosine_similarity},
            where=WorkflowJobLogNeighbours.cosine_similarity
            != insert_statement.excluded.cosine_similarity,
        )

        await session.execute(upsert_statement)

        await session.execute(
            sqlalchemy.update(cls)
            .where(cls.id.in_(job_ids))
            .values(neighbours_computed_at=sqlalchemy.func.now())
        )

    @classmethod
    async def get_failed_jobs(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository_id: github_types.GitHubRepositoryIdType,
        start_at: datetime.date | None,
        neighbour_cosine_similarity_threshold: float,
    ) -> sqlalchemy.Result[typing.Any]:
        tj_job = orm.aliased(WorkflowJobLogNeighbours, name="tj_job")
        job = orm.aliased(cls, name="job")
        job_rerun = orm.aliased(cls, name="job_rerun")

        stmt = (
            sqlalchemy.select(
                job.id,
                sqlalchemy.func.array_agg(
                    sqlalchemy.func.distinct(
                        sqlalchemy.case(
                            (job.id == tj_job.job_id, tj_job.neighbour_job_id),
                            else_=tj_job.job_id,
                        )
                    ),
                ).label("neighbour_job_ids"),
                job.name,
                job.embedded_log_error_title,
                job.workflow_run_id,
                job.steps,
                job.started_at,
                job.completed_at,
                job.run_attempt,
                sqlalchemy.func.bool_and(job_rerun.id.is_not(None)).label("flaky"),
            )
            .join(
                job_rerun,
                sqlalchemy.and_(
                    job_rerun.repository_id == job.repository_id,
                    job_rerun.name == job.name,
                    job_rerun.workflow_run_id == job.workflow_run_id,
                    job_rerun.run_attempt > job.run_attempt,
                    job_rerun.conclusion == WorkflowJobConclusion.SUCCESS,
                ),
                isouter=True,
            )
            .join(
                tj_job,
                sqlalchemy.and_(
                    job.id.in_((tj_job.job_id, tj_job.neighbour_job_id)),
                    tj_job.cosine_similarity >= neighbour_cosine_similarity_threshold,
                ),
                isouter=True,
            )
            .where(
                job.conclusion == WorkflowJobConclusion.FAILURE,
                job.repository_id == repository_id,
            )
            .group_by(job.id)
        )

        if start_at:
            stmt = stmt.where(job.started_at >= start_at)

        return await session.execute(stmt)

    @classmethod
    async def get_failed_job(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository_id: github_types.GitHubRepositoryIdType,
        job_id: int,
        neighbour_cosine_similarity_threshold: float,
    ) -> sqlalchemy.Result[typing.Any]:
        # NOTE(Kontrolix): This method is mostly a duplicate of get_failed_jobs
        # but the sql query is going to be drastically different in both of these
        # methods in the future, so no need to mutualise the code for now

        tj_job = orm.aliased(WorkflowJobLogNeighbours, name="tj_job")
        job = orm.aliased(cls, name="job")
        job_rerun = orm.aliased(cls, name="job_rerun")

        stmt = (
            sqlalchemy.select(
                job.id,
                sqlalchemy.func.array_agg(
                    sqlalchemy.func.distinct(
                        sqlalchemy.case(
                            (job.id == tj_job.job_id, tj_job.neighbour_job_id),
                            else_=tj_job.job_id,
                        )
                    ),
                ).label("neighbour_job_ids"),
                job.name,
                job.embedded_log_error_title,
                job.workflow_run_id,
                job.steps,
                job.started_at,
                job.completed_at,
                job.run_attempt,
                sqlalchemy.func.bool_and(job_rerun.id.is_not(None)).label("flaky"),
                job.embedded_log,
            )
            .join(
                job_rerun,
                sqlalchemy.and_(
                    job_rerun.repository_id == job.repository_id,
                    job_rerun.name == job.name,
                    job_rerun.workflow_run_id == job.workflow_run_id,
                    job_rerun.run_attempt > job.run_attempt,
                    job_rerun.conclusion == WorkflowJobConclusion.SUCCESS,
                ),
                isouter=True,
            )
            .join(
                tj_job,
                sqlalchemy.and_(
                    job.id.in_((tj_job.job_id, tj_job.neighbour_job_id)),
                    tj_job.cosine_similarity >= neighbour_cosine_similarity_threshold,
                ),
                isouter=True,
            )
            .where(
                job.id == job_id,
                job.repository_id == repository_id,
            )
            .group_by(job.id)
        )

        return await session.execute(stmt)


class WorkflowJobLogNeighbours(models.Base):
    __tablename__ = "gha_workflow_job_log_neighbours"

    job_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("gha_workflow_job.id"),
        primary_key=True,
        anonymizer_config=None,
        index=True,
    )
    neighbour_job_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("gha_workflow_job.id"),
        primary_key=True,
        anonymizer_config=None,
        index=True,
    )

    cosine_similarity: orm.Mapped[float] = orm.mapped_column(anonymizer_config=None)


# https://docs.sqlalchemy.org/en/20/orm/basic_relationships.html#association-object
class PullRequestWorkflowRunAssociation(models.Base):
    __tablename__ = "jt_gha_workflow_run_pull_request"

    pull_request_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("pull_request.id"),
        primary_key=True,
        anonymizer_config=None,
    )
    workflow_run_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("gha_workflow_run.id"),
        primary_key=True,
        anonymizer_config=None,
    )

    pull_request: orm.Mapped[PullRequest] = orm.relationship()
    workflow_run: orm.Mapped[WorkflowRun] = orm.relationship()

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        pull_request_id: int,
        workflow_run_id: int,
    ) -> None:
        sql = (
            postgresql.insert(cls)
            .values(pull_request_id=pull_request_id, workflow_run_id=workflow_run_id)
            .on_conflict_do_nothing(
                index_elements=["pull_request_id", "workflow_run_id"]
            )
        )
        await session.execute(sql)
