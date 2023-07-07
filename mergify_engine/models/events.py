from __future__ import annotations

import datetime
import enum
import typing

import sqlalchemy
from sqlalchemy import func
from sqlalchemy import orm
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine import signals
from mergify_engine.models import github_repository


class Event(models.Base):
    __tablename__ = "event"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_on": "type",
        "polymorphic_identity": "event.base",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger, primary_key=True, autoincrement=True
    )
    type: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text, index=True)
    received_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True), server_default=func.now()
    )
    pull_request: orm.Mapped[int] = orm.mapped_column(sqlalchemy.Integer, index=True)
    trigger: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)

    repository_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_repository.id")
    )
    repository: orm.Mapped[github_repository.GitHubRepository] = orm.relationship(
        lazy="joined"
    )

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository,
        pull_request: github_types.GitHubPullRequestNumber | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session, repository
        )

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            trigger=trigger,
            **metadata,
        )


class EventActionAssign(Event):
    __tablename__ = "event_action_assign"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.assign",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True
    )

    added: orm.Mapped[list[str]] = orm.mapped_column(
        sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1)
    )
    removed: orm.Mapped[list[str]] = orm.mapped_column(
        sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1)
    )


class CheckConclusion(str, enum.Enum):
    PENDING = None
    CANCELLED = "cancelled"
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"
    NEUTRAL = "neutral"
    STALE = "stale"
    ACTION_REQUIRED = "action_required"
    TIMED_OUT = "timed_out"


class EventActionPostCheck(Event):
    __tablename__ = "event_action_post_check"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.post_check",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True
    )
    conclusion: orm.Mapped[CheckConclusion] = orm.mapped_column(
        sqlalchemy.Enum(CheckConclusion), nullable=True
    )
    title: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)
    summary: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)


class EventActionCopy(Event):
    __tablename__ = "event_action_copy"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.copy",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True
    )
    to: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)
    pull_request_number: orm.Mapped[int] = orm.mapped_column(sqlalchemy.Integer)
    conflicts: orm.Mapped[bool] = orm.mapped_column(sqlalchemy.Boolean)


class EventActionComment(Event):
    __tablename__ = "event_action_comment"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.comment",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True
    )
    message: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)


class EventActionClose(Event):
    __tablename__ = "event_action_close"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.close",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True
    )
    message: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)
