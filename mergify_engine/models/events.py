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
from mergify_engine.rules.config import partition_rules


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


class EventActionDeleteHeadBranch(Event):
    __tablename__ = "event_action_delete_head_branch"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.delete_head_branch",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True
    )
    branch: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)


class EventActionDismissReviews(Event):
    __tablename__ = "event_action_dismiss_reviews"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.dismiss_reviews",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True
    )
    users: orm.Mapped[list[str]] = orm.mapped_column(
        sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1)
    )


class EventActionBackport(Event):
    __tablename__ = "event_action_backport"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.backport",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True
    )
    to: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)
    pull_request_number: orm.Mapped[int] = orm.mapped_column(sqlalchemy.Integer)
    conflicts: orm.Mapped[bool] = orm.mapped_column(sqlalchemy.Boolean)


class EventActionEdit(Event):
    __tablename__ = "event_action_edit"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.edit",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True
    )
    draft: orm.Mapped[bool] = orm.mapped_column(sqlalchemy.Boolean)


class EventActionLabel(Event):
    __tablename__ = "event_action_label"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.label",
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


class EventActionMerge(Event):
    __tablename__ = "event_action_merge"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.merge",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True
    )
    branch: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)


class EventActionQueueEnter(Event):
    __tablename__ = "event_action_queue_enter"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.queue.enter",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True
    )
    queue_name: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)
    branch: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)
    position: orm.Mapped[int] = orm.mapped_column(sqlalchemy.Integer)
    queued_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True)
    )
    partition_name: orm.Mapped[
        partition_rules.PartitionRuleName | None
    ] = orm.mapped_column(sqlalchemy.Text, nullable=True)


class EventActionQueueMerged(Event):
    __tablename__ = "event_action_queue_merged"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.queue.merged",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True
    )

    branch: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)
    queue_name: orm.Mapped[str] = orm.mapped_column(sqlalchemy.Text)
    queued_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True)
    )
    partition_names: orm.Mapped[
        list[partition_rules.PartitionRuleName]
    ] = orm.mapped_column(sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1))
