import datetime
import enum

import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine import models


class Account(models.Base):
    __tablename__ = "github_account"

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger, primary_key=True, autoincrement=False
    )
    login: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text, unique=True)


class PullRequest(models.Base):
    __tablename__ = "pull_request"

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger, primary_key=True, autoincrement=False
    )
    number: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger, unique=True)
    title: orm.Mapped[str]

    job_runs: orm.Mapped[list["JobRun"]] = orm.relationship(
        secondary="jt_gha_job_run_pull_request",
        back_populates="pull_requests",
        viewonly=True,
    )


class JobRunConclusion(enum.Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class JobRunTriggerEvent(enum.Enum):
    PULL_REQUEST = "pull_request"
    PULL_REQUEST_TARGET = "pull_request_target"
    PUSH = "push"
    SCHEDULE = "schedule"


class JobRunOperatingSystem(enum.Enum):
    LINUX = "Linux"
    MACOS = "macOS"
    WINDOWS = "Windows"


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

    owner: orm.Mapped[Account] = orm.relationship(
        lazy="joined", foreign_keys=[owner_id]
    )
    triggering_actor: orm.Mapped[Account] = orm.relationship(
        lazy="joined", foreign_keys=[triggering_actor_id]
    )
    pull_requests: orm.Mapped[list[PullRequest]] = orm.relationship(
        secondary="jt_gha_job_run_pull_request",
        back_populates="job_runs",
        viewonly=True,
        lazy="selectin",
    )


# https://docs.sqlalchemy.org/en/20/orm/basic_relationships.html#association-object
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
