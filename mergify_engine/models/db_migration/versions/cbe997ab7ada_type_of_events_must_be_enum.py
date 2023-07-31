"""Type of events must be enum

Revision ID: cbe997ab7ada
Revises: 7539200eded8
Create Date: 2023-07-28 11:48:14.563921

"""
import alembic
import sqlalchemy
from sqlalchemy.dialects import postgresql


revision = "cbe997ab7ada"
down_revision = "7539200eded8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    enum_values = [
        "action.assign",
        "action.backport",
        "action.close",
        "action.comment",
        "action.copy",
        "action.delete_head_branch",
        "action.dismiss_reviews",
        "action.edit",
        "action.label",
        "action.merge",
        "action.post_check",
        "action.queue.enter",
        "action.queue.checks_start",
        "action.queue.checks_end",
        "action.queue.leave",
        "action.queue.merged",
        "action.rebase",
        "action.refresh",
        "action.request_reviews",
        "action.requeue",
        "action.review",
        "action.squash",
        "action.unqueue",
        "action.update",
        "queue.freeze.create",
        "queue.freeze.update",
        "queue.freeze.delete",
        "queue.pause.create",
        "queue.pause.update",
        "queue.pause.delete",
    ]

    event_type = postgresql.ENUM(*enum_values, name="eventtype")
    event_type.create(alembic.op.get_bind())  # type: ignore [no-untyped-call]

    alembic.op.alter_column(
        "event",
        "type",
        existing_type=sqlalchemy.TEXT(),
        type_=sqlalchemy.Enum(
            *enum_values,
            name="eventtype",
        ),
        existing_nullable=False,
        postgresql_using="type::eventtype",
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
