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
        sqlalchemy.BigInteger, primary_key=True, autoincrement=False
    )
    number: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger)
    title: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)
    state: orm.Mapped[github_types.GitHubPullRequestState] = orm.mapped_column(
        sqlalchemy.Text
    )

    workflow_runs: orm.Mapped[list["WorkflowRun"]] = orm.relationship(
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


class JobRunConclusion(enum.Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"
    NEUTRAL = "neutral"
    TIMED_OUT = "timed_out"
    ACTION_REQUIRED = "action_required"


class JobRunTriggerEvent(enum.Enum):
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
        sqlalchemy.BigInteger, primary_key=True, autoincrement=False
    )
    workflow_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger)
    owner_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id")
    )
    event: orm.Mapped[JobRunTriggerEvent] = orm.mapped_column(
        sqlalchemy.Enum(JobRunTriggerEvent)
    )
    triggering_actor_id: orm.Mapped[int | None] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id")
    )
    run_attempt: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger)

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
        sqlalchemy.ForeignKey("github_repository.id")
    )

    repository: orm.Mapped[
        github_repository.GitHubRepository | None
    ] = orm.relationship(lazy="joined")

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        workflow_run_data: github_types.GitHubWorkflowRun,
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
                        session, workflow_run_data["repository"]
                    ),
                    event=JobRunTriggerEvent(workflow_run_data["event"]),
                    triggering_actor=triggering_actor,
                    run_attempt=workflow_run_data["run_attempt"],
                )
            )


class WorkflowJob(models.Base):
    __tablename__ = "gha_workflow_job"

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger, primary_key=True, autoincrement=False
    )
    workflow_run_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger)
    name: orm.Mapped[str]
    started_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True)
    )
    completed_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True)
    )
    conclusion: orm.Mapped[JobRunConclusion] = orm.mapped_column(
        sqlalchemy.Enum(JobRunConclusion)
    )
    labels: orm.Mapped[list[str]] = orm.mapped_column(
        sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1)
    )

    log_embedding: orm.Mapped[npt.NDArray[np.float32] | None] = orm.mapped_column(
        Vector(constants.OPENAI_EMBEDDING_DIMENSION), nullable=True
    )

    repository_id: orm.Mapped[github_types.GitHubRepositoryIdType] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_repository.id")
    )

    repository: orm.Mapped[
        github_repository.GitHubRepository | None
    ] = orm.relationship(lazy="joined")

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        workflow_job_data: github_types.GitHubJobRun,
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
                    conclusion=JobRunConclusion(workflow_job_data["conclusion"]),
                    labels=workflow_job_data["labels"],
                    repository=await github_repository.GitHubRepository.get_or_create(
                        session, repository
                    ),
                )
            )


# https://docs.sqlalchemy.org/en/20/orm/basic_relationships.html#association-object
class PullRequestWorkflowRunAssociation(models.Base):
    __tablename__ = "jt_gha_workflow_run_pull_request"

    pull_request_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("pull_request.id"), primary_key=True
    )
    workflow_run_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("gha_workflow_run.id"), primary_key=True
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
