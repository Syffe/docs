from __future__ import annotations

import datetime
import enum

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
            else:
                triggering_actor = None  # type: ignore[unreachable]

            session.add(
                cls(
                    id=workflow_run_data["id"],
                    workflow_id=workflow_run_data["workflow_id"],
                    owner=await github_account.GitHubAccount.get_or_create(
                        session, workflow_run_data["repository"]["owner"]
                    ),
                    repository=await github_repository.GitHubRepository.get_or_create(
                        session, repository
                    ),
                    event=WorkflowRunTriggerEvent(workflow_run_data["event"]),
                    triggering_actor=triggering_actor,
                    run_attempt=workflow_run_data["run_attempt"],
                )
            )


class WorkflowJob(models.Base):
    __tablename__ = "gha_workflow_job"

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

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        workflow_job_data: github_types.GitHubWorkflowJob,
        repository: github_types.GitHubRepository,
    ) -> None:
        result = await session.execute(
            sqlalchemy.select(cls).where(cls.id == workflow_job_data["id"])
        )
        if result.scalar_one_or_none() is None:
            session.add(
                cls(
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
                )
            )

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


class WorkflowJobLogNeighbours(models.Base):
    __tablename__ = "gha_workflow_job_log_neighbours"

    job_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("gha_workflow_job.id"),
        primary_key=True,
        anonymizer_config=None,
    )
    neighbour_job_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("gha_workflow_job.id"),
        primary_key=True,
        anonymizer_config=None,
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
