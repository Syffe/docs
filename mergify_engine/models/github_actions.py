import datetime
import enum

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.ci import models as ci_models
from mergify_engine.models import github_account


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

    job_runs: orm.Mapped[list["JobRun"]] = orm.relationship(
        secondary="jt_gha_job_run_pull_request",
        back_populates="pull_requests",
        viewonly=True,
    )
    workflow_runs: orm.Mapped[list["WorkflowRun"]] = orm.relationship(
        secondary="jt_gha_workflow_run_pull_request",
        back_populates="pull_requests",
        viewonly=True,
    )

    @classmethod
    async def insert(
        cls, session: sqlalchemy.ext.asyncio.AsyncSession, pull: ci_models.PullRequest
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


class JobRunOperatingSystem(enum.Enum):
    LINUX = "Linux"
    MACOS = "macOS"
    WINDOWS = "Windows"
    UNKNOWN = "Unknown"


# TODO(charly): remove this model once WorkflowRun and WorkflowJob fully replace it
class JobRun(models.Base):
    __tablename__ = "gha_job_run"
    __table_args__ = (
        sqlalchemy.Index(
            "gha_job_run_owner_id_repository_started_at_idx",
            "owner_id",
            "repository",
            "started_at",
        ),
        sqlalchemy.Index(
            "gha_job_run_owner_id_started_at_idx",
            "owner_id",
            "started_at",
        ),
    )

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger, primary_key=True, autoincrement=False
    )
    workflow_run_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger)
    workflow_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger)
    name: orm.Mapped[str]
    owner_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id")
    )
    repository: orm.Mapped[github_types.GitHubRepositoryName] = orm.mapped_column(
        sqlalchemy.Text
    )
    conclusion: orm.Mapped[JobRunConclusion] = orm.mapped_column(
        sqlalchemy.Enum(JobRunConclusion)
    )
    triggering_event: orm.Mapped[JobRunTriggerEvent] = orm.mapped_column(
        sqlalchemy.Enum(JobRunTriggerEvent)
    )
    triggering_actor_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id")
    )
    started_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True)
    )
    completed_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True)
    )
    run_attempt: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger)
    operating_system: orm.Mapped[JobRunOperatingSystem] = orm.mapped_column(
        sqlalchemy.Enum(JobRunOperatingSystem)
    )
    cores: orm.Mapped[int] = orm.mapped_column(sqlalchemy.Integer)

    owner: orm.Mapped[github_account.GitHubAccount] = orm.relationship(
        lazy="joined", foreign_keys=[owner_id]
    )
    triggering_actor: orm.Mapped[github_account.GitHubAccount] = orm.relationship(
        lazy="joined", foreign_keys=[triggering_actor_id]
    )
    pull_requests: orm.Mapped[list[PullRequest]] = orm.relationship(
        secondary="jt_gha_job_run_pull_request",
        back_populates="job_runs",
        viewonly=True,
        lazy="selectin",
    )


# TODO(charly): remove this model once PullRequestWorkflowRunAssociation fully replace it
class PullRequestJobRunAssociation(models.Base):
    __tablename__ = "jt_gha_job_run_pull_request"

    pull_request_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("pull_request.id"), primary_key=True
    )
    job_run_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("gha_job_run.id"), primary_key=True
    )

    job_run: orm.Mapped[JobRun] = orm.relationship()
    pull_request: orm.Mapped[PullRequest] = orm.relationship()


class WorkflowRun(models.Base):
    __tablename__ = "gha_workflow_run"
    __table_args__ = (
        sqlalchemy.Index(
            "gha_job_run_owner_id_repository_idx",
            "owner_id",
            "repository",
        ),
    )

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger, primary_key=True, autoincrement=False
    )
    workflow_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger)
    owner_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id")
    )
    repository: orm.Mapped[github_types.GitHubRepositoryName] = orm.mapped_column(
        sqlalchemy.Text
    )
    event: orm.Mapped[JobRunTriggerEvent] = orm.mapped_column(
        sqlalchemy.Enum(JobRunTriggerEvent)
    )
    triggering_actor_id: orm.Mapped[int] = orm.mapped_column(
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

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        workflow_run_data: github_types.GitHubWorkflowRun,
    ) -> None:
        sql = (
            postgresql.insert(cls)
            .values(
                id=workflow_run_data["id"],
                workflow_id=workflow_run_data["workflow_id"],
                owner_id=workflow_run_data["repository"]["owner"]["id"],
                repository=workflow_run_data["repository"]["name"],
                event=JobRunTriggerEvent(workflow_run_data["event"]),
                triggering_actor_id=workflow_run_data["triggering_actor"]["id"],
                run_attempt=workflow_run_data["run_attempt"],
            )
            .on_conflict_do_nothing(index_elements=["id"])
        )
        await session.execute(sql)


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

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        workflow_job_data: github_types.GitHubJobRun,
    ) -> None:
        sql = (
            postgresql.insert(cls)
            .values(
                id=workflow_job_data["id"],
                workflow_run_id=workflow_job_data["run_id"],
                name=workflow_job_data["name"],
                started_at=workflow_job_data["started_at"],
                completed_at=workflow_job_data["completed_at"],
                conclusion=JobRunConclusion(workflow_job_data["conclusion"]),
                labels=workflow_job_data["labels"],
            )
            .on_conflict_do_nothing(index_elements=["id"])
        )
        await session.execute(sql)


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
