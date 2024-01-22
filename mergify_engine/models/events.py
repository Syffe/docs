from __future__ import annotations

import datetime  # noqa: TCH003
import typing

import sqlalchemy
from sqlalchemy import func
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.hybrid
import sqlalchemy.sql.elements
import sqlalchemy.sql.functions

from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import models
from mergify_engine import pagination
from mergify_engine import signals
from mergify_engine import utils
from mergify_engine.clients import github  # noqa: TCH001
from mergify_engine.models import enumerations
from mergify_engine.models import events_metadata
from mergify_engine.models.github import repository as github_repository
from mergify_engine.rules.config import partition_rules  # noqa: TCH001
from mergify_engine.rules.config import queue_rules as qr_config  # noqa: TCH001


if typing.TYPE_CHECKING:
    from mergify_engine import context


class Event(models.Base):
    __tablename__ = "event"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_on": "type",
        "polymorphic_identity": "event.base",
    }
    __repr_attributes__: typing.ClassVar[tuple[str, ...]] = ("id", "type")
    __table_args__ = (
        sqlalchemy.Index("event_repository_id_type_idx", "repository_id", "type"),
        sqlalchemy.Index(
            "event_repository_id_type_received_at_idx",
            "repository_id",
            "type",
            "received_at",
        ),
    )

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=True,
        anonymizer_config=None,
    )
    type: orm.Mapped[enumerations.EventType] = orm.mapped_column(
        sqlalchemy.Enum(
            enumerations.EventType,
            values_callable=lambda x: [e.value for e in x],
        ),
        index=True,
        anonymizer_config="anon.random_in_enum(type)",
    )
    received_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        server_default=func.now(),
        default=date.utcnow,
        anonymizer_config="anon.dnoise(received_at, ''2 days'')",
        # NOTE(sileht): Required by delete_outdated()
        index=True,
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
        sqlalchemy.ForeignKey("github_repository.id"),
        anonymizer_config=None,
        index=True,
    )
    repository: orm.Mapped[github_repository.GitHubRepository] = orm.relationship(
        lazy="joined",
    )

    base_ref: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository
        | github_repository.GitHubRepositoryDict,
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session,
            repository,
        )

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            base_ref=base_ref,
            trigger=trigger,
            **metadata,
        )

    @classmethod
    async def get(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        cursor: pagination.Cursor,
        limit: int,
        repository_ctxt: context.Repository,
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        event_type: list[enumerations.EventType] | None,
        received_from: datetime.datetime | None,
        received_to: datetime.datetime | None,
    ) -> typing.Sequence[Event]:
        filter_dict = {
            "repository_id": cls.repository_id == repository_ctxt.repo["id"],
            "pull_request": cls.pull_request == pull_request
            if pull_request is not None
            else None,
            "base_ref": cls.base_ref == base_ref if base_ref is not None else None,
            "event_type": cls.type.in_(event_type) if event_type is not None else None,
            "received_from": cls.received_at >= received_from
            if received_from is not None
            else None,
            "received_to": cls.received_at <= received_to
            if received_to is not None
            else None,
        }

        if cursor.value:
            try:
                event_id = int(cursor.value)
            except ValueError:
                raise pagination.InvalidCursorError(cursor)

            if cursor.forward:
                filter_dict.update({"cursor": cls.id < event_id})
            else:
                filter_dict.update({"cursor": cls.id > event_id})

        subquery = (
            sqlalchemy.select(cls)
            .where(*[f for f in filter_dict.values() if f is not None])
            .limit(limit)
            .order_by(cls.id.desc() if cursor.forward else cls.id.asc())
        ).subquery()
        events_aliased = orm.aliased(cls, subquery)

        return (
            await session.scalars(
                sqlalchemy.select(events_aliased)
                .order_by(events_aliased.id.desc())
                .options(
                    orm.selectin_polymorphic(events_aliased, cls.__subclasses__()),
                ),
            )
        ).all()

    @classmethod
    async def delete_outdated(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        retention_time: datetime.timedelta,
    ) -> None:
        stmt = sqlalchemy.delete(cls).where(
            cls.received_at < (date.utcnow() - retention_time),
        )
        await session.execute(stmt)


class EventActionAssign(Event):
    __tablename__ = "event_action_assign"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.assign",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )

    added: orm.Mapped[list[str]] = orm.mapped_column(
        postgresql.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(0, 5, 20)",
    )
    removed: orm.Mapped[list[str]] = orm.mapped_column(
        postgresql.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(0, 5, 20)",
    )


class EventActionPostCheck(Event):
    __tablename__ = "event_action_post_check"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.post_check",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
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
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    to: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    pull_request_number: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer,
        anonymizer_config="anon.random_int_between(0,100000)",
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
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    message: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 20)",
    )


class EventActionClose(Event):
    __tablename__ = "event_action_close"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.close",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    message: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 20)",
    )


class EventActionDeleteHeadBranch(Event):
    __tablename__ = "event_action_delete_head_branch"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.delete_head_branch",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    branch: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )


class EventActionDismissReviews(Event):
    __tablename__ = "event_action_dismiss_reviews"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.dismiss_reviews",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    users: orm.Mapped[list[str]] = orm.mapped_column(
        postgresql.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(0, 5, 20)",
    )


class EventActionBackport(Event):
    __tablename__ = "event_action_backport"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.backport",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    to: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    pull_request_number: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer,
        anonymizer_config="anon.random_int_between(0,100000)",
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
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
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
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    added: orm.Mapped[list[str]] = orm.mapped_column(
        postgresql.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(1, 2, 7)",
    )
    removed: orm.Mapped[list[str]] = orm.mapped_column(
        postgresql.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(1, 2, 7)",
    )


class EventActionMerge(Event):
    __tablename__ = "event_action_merge"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.merge",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    branch: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )


class EventActionGithubActions(Event):
    __tablename__ = "event_action_github_actions"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.github_actions",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    workflow: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    inputs: orm.Mapped[dict[str, str | int | bool]] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config=(
            "custom_masks.jsonb_obj(0, 3, ARRAY[''text'', ''integer'', ''boolean''])"
        ),
    )


class EventActionQueueEnter(Event):
    __tablename__ = "event_action_queue_enter"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.queue.enter",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    queue_name: orm.Mapped[qr_config.QueueName] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    branch: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    position: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer,
        anonymizer_config="anon.random_int_between(0, 50)",
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
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    branch: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    queue_name: orm.Mapped[qr_config.QueueName] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    queued_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        anonymizer_config="anon.dnoise(queued_at, ''2 days'')",
    )
    partition_names: orm.Mapped[
        list[partition_rules.PartitionRuleName]
    ] = orm.mapped_column(
        postgresql.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(1, 2, 7)",
    )

    @classmethod
    async def get_merged_count_by_interval(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository_ctxt: context.Repository,
        timerange: utils.TimeRange,
        base_ref: list[github_types.GitHubRefType] | None = None,
        partition_name: list[partition_rules.PartitionRuleName] | None = None,
        queue_name: list[qr_config.QueueName] | None = None,
    ) -> typing.Sequence[sqlalchemy.engine.row.Row[typing.Any]]:
        filters = {
            cls.received_at >= timerange.start_at,
            cls.received_at <= timerange.end_at,
            cls.repository_id == repository_ctxt.repo["id"],
        }
        interval = timerange.get_pg_interval()

        if base_ref is not None:
            filters.add(cls.base_ref.in_(base_ref))
        if partition_name is not None:
            filters.add(cls.partition_names.overlap(partition_name))
        if queue_name is not None:
            filters.add(cls.queue_name.in_(queue_name))

        chunk = func.date_bin(
            sqlalchemy.cast(interval[0], postgresql.INTERVAL),
            sqlalchemy.cast(cls.received_at, postgresql.TIMESTAMP(timezone=True)),
            sqlalchemy.cast(date.EPOCH, postgresql.TIMESTAMP(timezone=True)),
        )

        stmt = (
            sqlalchemy.select(
                chunk.label("start"),
                (chunk + interval[1]).label("end"),
                func.count().label("merged"),
                cls.base_ref,
                cls.partition_names,
                cls.queue_name,
            )
            .select_from(cls)
            .where(*filters)
            .group_by(chunk, cls.base_ref, cls.partition_names, cls.queue_name)
            .order_by(cls.base_ref, cls.partition_names, cls.queue_name, chunk)
        )

        result = await session.execute(stmt)
        return result.all()


class EventActionQueueLeave(Event):
    __tablename__ = "event_action_queue_leave"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.queue.leave",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    queue_name: orm.Mapped[qr_config.QueueName] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    branch: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    position: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer,
        anonymizer_config="anon.random_int_between(0, 50)",
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
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 7)",
    )
    seconds_waiting_for_schedule: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer,
        anonymizer_config=None,
    )
    seconds_waiting_for_freeze: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer,
        anonymizer_config=None,
    )

    @sqlalchemy.ext.hybrid.hybrid_property
    def queued_time(self) -> float:
        return (self.received_at - self.queued_at).total_seconds()

    @queued_time.inplace.expression
    @classmethod
    def _queued_time_expression(cls) -> sqlalchemy.ColumnElement[float]:
        return sqlalchemy.type_coerce(
            sqlalchemy.extract(
                "epoch",
                cls.received_at - cls.queued_at,
            ),
            sqlalchemy.Float,
        ).label("queued_time")

    @classmethod
    async def get_average_idle_queue_time_by_interval(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository_ctxt: context.Repository,
        timerange: utils.TimeRange,
        base_ref: list[github_types.GitHubRefType] | None = None,
        partition_name: list[partition_rules.PartitionRuleName] | None = None,
        queue_name: list[qr_config.QueueName] | None = None,
    ) -> typing.Sequence[sqlalchemy.engine.row.Row[typing.Any]]:
        filters = {
            cls.received_at >= timerange.start_at,
            cls.received_at <= timerange.end_at,
            cls.repository_id == repository_ctxt.repo["id"],
        }
        interval = timerange.get_pg_interval()

        if base_ref is not None:
            filters.add(cls.base_ref.in_(base_ref))
        if partition_name is not None:
            filters.add(cls.partition_name.in_(partition_name))
        if queue_name is not None:
            filters.add(cls.queue_name.in_(queue_name))

        chunk = func.date_bin(
            sqlalchemy.cast(interval[0], postgresql.INTERVAL),
            sqlalchemy.cast(cls.received_at, postgresql.TIMESTAMP(timezone=True)),
            sqlalchemy.cast(date.EPOCH, postgresql.TIMESTAMP(timezone=True)),
        )

        spec_check = events_metadata.SpeculativeCheckPullRequest
        checks_end = orm.aliased(EventActionQueueChecksEnd, name="checks_end")

        # Idle means we don't want the CI runtime in the result
        ci_runtime = (
            sqlalchemy.select(spec_check.ci_runtime)
            .join(
                checks_end,
                checks_end.id == spec_check.event_id,
            )
            .where(
                checks_end.pull_request == cls.pull_request,
                checks_end.repository_id == cls.repository_id,
                checks_end.received_at <= cls.received_at,
            )
            .order_by(checks_end.received_at.desc())
            .limit(1)
            .scalar_subquery()
        )

        stmt = (
            sqlalchemy.select(
                chunk.label("start"),
                (chunk + interval[1]).label("end"),
                sqlalchemy.type_coerce(
                    sqlalchemy.func.avg(
                        cls.queued_time
                        - sqlalchemy.func.coalesce(ci_runtime, 0.0)
                        - cls.seconds_waiting_for_freeze,
                    ),
                    sqlalchemy.Float,
                ).label("queued_idle_time"),
                cls.base_ref,
                cls.partition_name,
                cls.queue_name,
            )
            .where(*filters)
            .group_by(
                chunk,
                cls.base_ref,
                cls.partition_name,
                cls.queue_name,
            )
            .order_by(cls.base_ref, cls.partition_name, cls.queue_name, chunk)
        )

        result = await session.execute(stmt)
        return result.all()


class EventActionQueueChange(Event):
    __tablename__ = "event_action_queue_change"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.queue.change",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    queue_name: orm.Mapped[qr_config.QueueName] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    partition_name: orm.Mapped[
        partition_rules.PartitionRuleName | None
    ] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    size: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer,
        anonymizer_config=None,
    )
    running_checks: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer,
        anonymizer_config=None,
    )

    @classmethod
    async def get_queue_size_by_interval(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository_ctxt: context.Repository,
        timerange: utils.TimeRange,
        base_ref: list[github_types.GitHubRefType] | None = None,
        partition_name: list[partition_rules.PartitionRuleName] | None = None,
        queue_name: list[qr_config.QueueName] | None = None,
    ) -> typing.Sequence[sqlalchemy.engine.row.Row[typing.Any]]:
        filters = {
            cls.received_at >= timerange.start_at,
            cls.received_at <= timerange.end_at,
            cls.repository_id == repository_ctxt.repo["id"],
        }

        interval = timerange.get_pg_interval()

        if base_ref is not None:
            filters.add(cls.base_ref.in_(base_ref))
        if partition_name is not None:
            filters.add(cls.partition_name.in_(partition_name))
        if queue_name is not None:
            filters.add(cls.queue_name.in_(queue_name))

        chunk = func.date_bin(
            sqlalchemy.cast(interval[0], postgresql.INTERVAL),
            sqlalchemy.cast(cls.received_at, postgresql.TIMESTAMP(timezone=True)),
            sqlalchemy.cast(date.EPOCH, postgresql.TIMESTAMP(timezone=True)),
        )

        stmt = (
            sqlalchemy.select(
                chunk.label("start"),
                (chunk + interval[1]).label("end"),
                sqlalchemy.func.round(
                    sqlalchemy.func.avg(cls.size),
                    2,
                ).label("avg_size"),
                sqlalchemy.func.max(cls.size).label("max_size"),
                sqlalchemy.func.min(cls.size).label("min_size"),
                cls.base_ref,
                cls.partition_name,
                cls.queue_name,
            )
            .where(*filters)
            .group_by(
                chunk,
                cls.base_ref,
                cls.partition_name,
                cls.queue_name,
            )
            .order_by(cls.base_ref, cls.partition_name, cls.queue_name, chunk)
        )

        result = await session.execute(stmt)
        return result.all()


class EventActionSquash(Event):
    __tablename__ = "event_action_squash"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.squash",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )


class EventActionRebase(Event):
    __tablename__ = "event_action_rebase"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.rebase",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )


class EventActionRefresh(Event):
    __tablename__ = "event_action_refresh"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.refresh",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )


class EventActionRequeue(Event):
    __tablename__ = "event_action_requeue"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.requeue",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )


class EventActionUnqueue(Event):
    __tablename__ = "event_action_unqueue"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.unqueue",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )


class EventActionUpdate(Event):
    __tablename__ = "event_action_update"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.update",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )


class EventActionQueueChecksStart(Event):
    __tablename__ = "event_action_queue_checks_start"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.queue.checks_start",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    branch: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    partition_name: orm.Mapped[
        partition_rules.PartitionRuleName | None
    ] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    position: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer,
        anonymizer_config="anon.random_int_between(0, 50)",
    )
    queue_name: orm.Mapped[qr_config.QueueName] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    queued_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        anonymizer_config="anon.dnoise(queued_at, ''2 days'')",
    )
    start_reason: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 7)",
        server_default="",
    )
    speculative_check_pull_request: orm.Mapped[
        events_metadata.SpeculativeCheckPullRequest
    ] = orm.relationship(
        lazy="joined",
        single_parent=True,
        cascade="all, delete-orphan",
    )

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository
        | github_repository.GitHubRepositoryDict,
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session,
            repository,
        )

        metadata = typing.cast(signals.EventQueueChecksStartMetadata, metadata.copy())
        speculative_check_pull_request = events_metadata.SpeculativeCheckPullRequest(
            **metadata.pop("speculative_check_pull_request"),
        )

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            base_ref=base_ref,
            trigger=trigger,
            speculative_check_pull_request=speculative_check_pull_request,
            **metadata,
        )


class EventActionQueueChecksEnd(Event):
    __tablename__ = "event_action_queue_checks_end"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.queue.checks_end",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    branch: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    partition_name: orm.Mapped[
        partition_rules.PartitionRuleName | None
    ] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    position: orm.Mapped[int | None] = orm.mapped_column(
        sqlalchemy.Integer,
        anonymizer_config="anon.random_int_between(0, 50)",
    )
    queue_name: orm.Mapped[qr_config.QueueName] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7)",
    )
    queued_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        anonymizer_config="anon.dnoise(queued_at, ''2 days'')",
    )

    aborted: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )
    abort_code: orm.Mapped[
        enumerations.QueueChecksAbortCode | None
    ] = orm.mapped_column(
        sqlalchemy.Enum(enumerations.QueueChecksAbortCode),
        anonymizer_config="anon.random_in_enum(abort_code)",
    )
    abort_reason: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )
    abort_status: orm.Mapped[enumerations.QueueChecksAbortStatus] = orm.mapped_column(
        sqlalchemy.Enum(enumerations.QueueChecksAbortStatus),
        anonymizer_config="anon.random_in_enum(abort_status)",
    )

    speculative_check_pull_request: orm.Mapped[
        events_metadata.SpeculativeCheckPullRequest
    ] = orm.relationship(
        lazy="joined",
        single_parent=True,
        cascade="all, delete-orphan",
    )

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository
        | github_repository.GitHubRepositoryDict,
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session,
            repository,
        )

        metadata = typing.cast(signals.EventQueueChecksEndMetadata, metadata.copy())
        speculative_check_pull_request = events_metadata.SpeculativeCheckPullRequest(
            **metadata.pop("speculative_check_pull_request"),
        )

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            base_ref=base_ref,
            trigger=trigger,
            speculative_check_pull_request=speculative_check_pull_request,
            **metadata,
        )

    @classmethod
    async def get_average_ci_runtime_by_interval(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository_ctxt: context.Repository,
        timerange: utils.TimeRange,
        base_ref: list[github_types.GitHubRefType] | None = None,
        partition_name: list[partition_rules.PartitionRuleName] | None = None,
        queue_name: list[qr_config.QueueName] | None = None,
    ) -> typing.Sequence[sqlalchemy.engine.row.Row[typing.Any]]:
        # shortcut for a more readable query
        spec_check = events_metadata.SpeculativeCheckPullRequest

        filters = {
            sqlalchemy.and_(
                spec_check.checks_started_at.isnot(None),
                spec_check.checks_ended_at.isnot(None),
            ),
            spec_check.checks_ended_at >= timerange.start_at,
            spec_check.checks_ended_at <= timerange.end_at,
            cls.repository_id == repository_ctxt.repo["id"],
        }
        interval = timerange.get_pg_interval()

        if base_ref is not None:
            filters.add(cls.base_ref.in_(base_ref))
        if partition_name is not None:
            filters.add(cls.partition_name.in_(partition_name))
        if queue_name is not None:
            filters.add(cls.queue_name.in_(queue_name))

        chunk = func.date_bin(
            sqlalchemy.cast(interval[0], postgresql.INTERVAL),
            sqlalchemy.cast(
                spec_check.checks_ended_at,
                postgresql.TIMESTAMP(timezone=True),
            ),
            sqlalchemy.cast(date.EPOCH, postgresql.TIMESTAMP(timezone=True)),
        )

        stmt = (
            sqlalchemy.select(
                chunk.label("start"),
                (chunk + interval[1]).label("end"),
                sqlalchemy.type_coerce(
                    sqlalchemy.func.avg(spec_check.ci_runtime),
                    sqlalchemy.Float,
                ).label("runtime"),
                cls.base_ref,
                cls.partition_name,
                cls.queue_name,
            )
            .join(cls, cls.id == spec_check.event_id)
            .where(*filters)
            .group_by(
                chunk,
                cls.base_ref,
                cls.partition_name,
                cls.queue_name,
            )
            .order_by(cls.base_ref, cls.partition_name, cls.queue_name, chunk)
        )

        result = await session.execute(stmt)
        return result.all()


class EventActionRequestReviews(Event):
    __tablename__ = "event_action_request_reviews"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.request_reviews",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )

    reviewers: orm.Mapped[list[str]] = orm.mapped_column(
        postgresql.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(0, 5, 8)",
    )
    team_reviewers: orm.Mapped[list[str]] = orm.mapped_column(
        postgresql.ARRAY(sqlalchemy.Text, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(0, 5, 8)",
    )


class EventActionReview(Event):
    __tablename__ = "event_action_review"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "action.review",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
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
        base_ref: github_types.GitHubRefType | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        # NOTE(lecrepont01): field `type` already exists on base class as discriminator,
        # meaning that it must be renamed `review_type` in the relation
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session,
            repository,
        )
        metadata = typing.cast(signals.EventReviewMetadata, metadata)

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            base_ref=base_ref,
            trigger=trigger,
            review_type=metadata["review_type"],
            reviewer=metadata["reviewer"],
            message=metadata["message"],
        )


class EventQueueFreezeCreate(Event):
    __tablename__ = "event_queue_freeze_create"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "queue.freeze.create",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    queue_name: orm.Mapped[qr_config.QueueName] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    reason: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )
    cascading: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )

    created_by: orm.Mapped[github.Actor] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config="custom_masks.jsonb_obj(3, 3, ARRAY[''text''])",
    )

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository
        | github_repository.GitHubRepositoryDict,
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session,
            repository,
        )

        metadata = typing.cast(signals.EventQueueFreezeCreateMetadata, metadata.copy())
        created_by = metadata.pop("created_by")

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            base_ref=base_ref,
            trigger=trigger,
            created_by=created_by,
            **metadata,
        )


class EventQueueFreezeUpdate(Event):
    __tablename__ = "event_queue_freeze_update"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "queue.freeze.update",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    queue_name: orm.Mapped[qr_config.QueueName] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    reason: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )
    cascading: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )

    updated_by: orm.Mapped[github.Actor] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config="custom_masks.jsonb_obj(3, 3, ARRAY[''text''])",
    )

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository
        | github_repository.GitHubRepositoryDict,
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session,
            repository,
        )

        metadata = typing.cast(signals.EventQueueFreezeUpdateMetadata, metadata.copy())
        updated_by = metadata.pop("updated_by")

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            base_ref=base_ref,
            trigger=trigger,
            updated_by=updated_by,
            **metadata,
        )


class EventQueueFreezeDelete(Event):
    __tablename__ = "event_queue_freeze_delete"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "queue.freeze.delete",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    queue_name: orm.Mapped[qr_config.QueueName] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )

    deleted_by: orm.Mapped[github.Actor] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config="custom_masks.jsonb_obj(3, 3, ARRAY[''text''])",
    )

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository
        | github_repository.GitHubRepositoryDict,
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session,
            repository,
        )

        metadata = typing.cast(signals.EventQueueFreezeDeleteMetadata, metadata.copy())
        deleted_by = metadata.pop("deleted_by")

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            base_ref=base_ref,
            trigger=trigger,
            deleted_by=deleted_by,
            **metadata,
        )


class EventQueuePauseCreate(Event):
    __tablename__ = "event_queue_pause_create"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "queue.pause.create",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    reason: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )

    created_by: orm.Mapped[github.Actor] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config="custom_masks.jsonb_obj(3, 3, ARRAY[''text''])",
    )

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository
        | github_repository.GitHubRepositoryDict,
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session,
            repository,
        )

        metadata = typing.cast(signals.EventQueuePauseCreateMetadata, metadata.copy())
        created_by = metadata.pop("created_by")

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            base_ref=base_ref,
            trigger=trigger,
            created_by=created_by,
            **metadata,
        )


class EventQueuePauseUpdate(Event):
    __tablename__ = "event_queue_pause_update"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "queue.pause.update",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )
    reason: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )

    updated_by: orm.Mapped[github.Actor] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config="custom_masks.jsonb_obj(3, 3, ARRAY[''text''])",
    )

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository
        | github_repository.GitHubRepositoryDict,
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session,
            repository,
        )

        metadata = typing.cast(signals.EventQueuePauseUpdateMetadata, metadata.copy())
        updated_by = metadata.pop("updated_by")

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            base_ref=base_ref,
            trigger=trigger,
            updated_by=updated_by,
            **metadata,
        )


class EventQueuePauseDelete(Event):
    __tablename__ = "event_queue_pause_delete"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore [misc]
        "polymorphic_identity": "queue.pause.delete",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        primary_key=True,
        anonymizer_config=None,
    )

    deleted_by: orm.Mapped[github.Actor] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config="custom_masks.jsonb_obj(3, 3, ARRAY[''text''])",
    )

    @classmethod
    async def create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository
        | github_repository.GitHubRepositoryDict,
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        trigger: str,
        metadata: signals.EventMetadata,
    ) -> Event:
        repository_obj = await github_repository.GitHubRepository.get_or_create(
            session,
            repository,
        )

        metadata = typing.cast(signals.EventQueuePauseDeleteMetadata, metadata.copy())
        deleted_by = metadata.pop("deleted_by")

        return cls(
            repository=repository_obj,
            pull_request=pull_request,
            base_ref=base_ref,
            trigger=trigger,
            deleted_by=deleted_by,
            **metadata,
        )
