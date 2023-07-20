from __future__ import annotations

import datetime
import typing

import sqlalchemy
from sqlalchemy import func
from sqlalchemy import orm
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine import signals
from mergify_engine.models import enumerations
from mergify_engine.models import events_metadata
from mergify_engine.models import github_repository
from mergify_engine.rules.config import partition_rules


class Event(models.Base):
    __tablename__ = "event"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_on": "type",
        "polymorphic_identity": "event.base",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=True,
        anonymizer_config=None,
    )
    type: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        index=True,
        # FIXME(sileht): must be an enum MRGFY-2435
        # anonymizer_config="anon.random_in_enum(type)",
        anonymizer_config=None,
    )
    received_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        server_default=func.now(),
        anonymizer_config="anon.dnoise(received_at, ''2 days'')",
    )
    pull_request: orm.Mapped[int | None] = orm.mapped_column(
        sqlalchemy.Integer,
        index=True,
        anonymizer_config="anon.random_int_between(1,100000)",
    )
    trigger: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )

    repository_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_repository.id"), anonymizer_config=None
    )
    repository: orm.Mapped[github_repository.GitHubRepository] = orm.relationship(
        lazy="joined"
    )

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository
        | github_repository.GitHubRepositoryDict,
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
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )

    added: orm.Mapped[list[str]] = orm.mapped_column(
        sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(0, 5, 20)",
    )
    removed: orm.Mapped[list[str]] = orm.mapped_column(
        sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(0, 5, 20)",
    )


class EventActionPostCheck(Event):
    __tablename__ = "event_action_post_check"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.post_check",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    conclusion: orm.Mapped[enumerations.CheckConclusion] = orm.mapped_column(
        sqlalchemy.Enum(enumerations.CheckConclusion),
        nullable=True,
        anonymizer_config="anon.random_in_enum(conclusion)",
    )
    title: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )
    summary: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )


class EventActionCopy(Event):
    __tablename__ = "event_action_copy"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.copy",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    to: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    pull_request_number: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer, anonymizer_config="anon.random_int_between(0,100000)"
    )
    conflicts: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )


class EventActionComment(Event):
    __tablename__ = "event_action_comment"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.comment",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    message: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( words := 20)"
    )


class EventActionClose(Event):
    __tablename__ = "event_action_close"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.close",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    message: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( words := 20)"
    )


class EventActionDeleteHeadBranch(Event):
    __tablename__ = "event_action_delete_head_branch"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.delete_head_branch",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    branch: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( characters := 7)"
    )


class EventActionDismissReviews(Event):
    __tablename__ = "event_action_dismiss_reviews"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.dismiss_reviews",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    users: orm.Mapped[list[str]] = orm.mapped_column(
        sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(0, 5, 20)",
    )


class EventActionBackport(Event):
    __tablename__ = "event_action_backport"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.backport",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    to: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    pull_request_number: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer, anonymizer_config="anon.random_int_between(0,100000)"
    )
    conflicts: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )


class EventActionEdit(Event):
    __tablename__ = "event_action_edit"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.edit",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    draft: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )


class EventActionLabel(Event):
    __tablename__ = "event_action_label"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.label",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    added: orm.Mapped[list[str]] = orm.mapped_column(
        sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(1, 2, 7)",
    )
    removed: orm.Mapped[list[str]] = orm.mapped_column(
        sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(1, 2, 7)",
    )


class EventActionMerge(Event):
    __tablename__ = "event_action_merge"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.merge",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    branch: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( characters := 7)"
    )


class EventActionQueueEnter(Event):
    __tablename__ = "event_action_queue_enter"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.queue.enter",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    queue_name: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( characters := 7)"
    )
    branch: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( characters := 7)"
    )
    position: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer, anonymizer_config="anon.random_int_between(0, 50)"
    )
    queued_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        anonymizer_config="anon.dnoise(queued_at, ''2 days'')",
    )
    partition_name: orm.Mapped[
        partition_rules.PartitionRuleName | None
    ] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )


class EventActionQueueMerged(Event):
    __tablename__ = "event_action_queue_merged"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.queue.merged",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    branch: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( characters := 7)"
    )
    queue_name: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( characters := 7)"
    )
    queued_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        anonymizer_config="anon.dnoise(queued_at, ''2 days'')",
    )
    partition_names: orm.Mapped[
        list[partition_rules.PartitionRuleName]
    ] = orm.mapped_column(
        sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(1, 2, 7)",
    )


class EventActionQueueLeave(Event):
    __tablename__ = "event_action_queue_leave"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.queue.leave",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    queue_name: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( characters := 7)"
    )
    branch: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( characters := 7)"
    )
    position: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer, anonymizer_config="anon.random_int_between(0, 50)"
    )
    queued_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        anonymizer_config="anon.dnoise(queued_at, ''2 days'')",
    )
    partition_name: orm.Mapped[
        partition_rules.PartitionRuleName | None
    ] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    merged: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )
    reason: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( words := 7)"
    )
    seconds_waiting_for_schedule: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer, anonymizer_config=None
    )
    seconds_waiting_for_freeze: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer, anonymizer_config=None
    )


class EventActionSquash(Event):
    __tablename__ = "event_action_squash"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.squash",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )


class EventActionRebase(Event):
    __tablename__ = "event_action_rebase"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.rebase",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )


class EventActionRefresh(Event):
    __tablename__ = "event_action_refresh"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.refresh",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )


class EventActionRequeue(Event):
    __tablename__ = "event_action_requeue"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.requeue",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )


class EventActionUnqueue(Event):
    __tablename__ = "event_action_unqueue"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.unqueue",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )


class EventActionUpdate(Event):
    __tablename__ = "event_action_update"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.update",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )


class EventActionQueueChecksStart(Event):
    __tablename__ = "event_action_queue_checks_start"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.queue.checks_start",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    branch: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( characters := 7)"
    )
    partition_name: orm.Mapped[
        partition_rules.PartitionRuleName | None
    ] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    position: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer, anonymizer_config="anon.random_int_between(0, 50)"
    )
    queue_name: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( characters := 7)"
    )
    queued_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        anonymizer_config="anon.dnoise(queued_at, ''2 days'')",
    )

    speculative_check_pull_request_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("speculative_check_pull_request.id"),
        anonymizer_config=None,
    )
    speculative_check_pull_request: orm.Mapped[
        events_metadata.SpeculativeCheckPullRequest
    ] = orm.relationship(lazy="joined")

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository
        | github_repository.GitHubRepositoryDict,
        pull_request: github_types.GitHubPullRequestNumber | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session, repository
        )

        metadata = typing.cast(signals.EventQueueChecksStartMetadata, metadata)
        speculative_check_pull_request = events_metadata.SpeculativeCheckPullRequest(
            **metadata.pop("speculative_check_pull_request")
        )

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            trigger=trigger,
            speculative_check_pull_request=speculative_check_pull_request,
            **metadata,
        )


class EventActionRequestReviews(Event):
    __tablename__ = "event_action_request_reviews"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.request_reviews",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"),
        primary_key=True,
        anonymizer_config=None,
    )

    reviewers: orm.Mapped[list[str]] = orm.mapped_column(
        sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(0, 5, 8)",
    )
    team_reviewers: orm.Mapped[list[str]] = orm.mapped_column(
        sqlalchemy.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(0, 5, 8)",
    )


class EventActionReview(Event):
    __tablename__ = "event_action_review"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.review",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"),
        primary_key=True,
        anonymizer_config=None,
    )
    review_type: orm.Mapped[enumerations.ReviewType | None] = orm.mapped_column(
        sqlalchemy.Enum(enumerations.ReviewType),
        nullable=True,
        anonymizer_config="anon.random_in_enum(review_type)",
    )
    reviewer: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    message: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository
        | github_repository.GitHubRepositoryDict,
        pull_request: github_types.GitHubPullRequestNumber | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        # NOTE(lecrepont01): field `type` already exists on base class as discriminator,
        # meaning that it must be renamed `review_type` in the relation
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session, repository
        )
        metadata = typing.cast(signals.EventReviewMetadata, metadata)

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            trigger=trigger,
            review_type=metadata["type"],
            reviewer=metadata["reviewer"],
            message=metadata["message"],
        )


class EventQueueFreezeCreate(Event):
    __tablename__ = "event_queue_freeze_create"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "queue.freeze.create",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id"), primary_key=True, anonymizer_config=None
    )
    queue_name: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text, anonymizer_config="anon.lorem_ipsum( characters := 7 )"
    )
    reason: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )
    cascading: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )

    created_by_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_authenticated_actor.id"), anonymizer_config=None
    )
    created_by: orm.Mapped[events_metadata.GithubAuthenticatedActor] = orm.relationship(
        lazy="joined"
    )

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository
        | github_repository.GitHubRepositoryDict,
        pull_request: github_types.GitHubPullRequestNumber | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session, repository
        )

        metadata = typing.cast(signals.EventQueueFreezeCreateMetadata, metadata)
        actor = await events_metadata.GithubAuthenticatedActor.get_or_create(
            session,
            metadata.pop("created_by"),
        )

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            trigger=trigger,
            created_by=actor,
            **metadata,
        )
