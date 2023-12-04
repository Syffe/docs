"""add event/received_at index

Revision ID: 86d40725a53e
Revises: d9020c768f39
Create Date: 2023-12-04 10:40:10.111970

"""
import alembic


revision = "86d40725a53e"
down_revision = "d9020c768f39"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.create_index(
        alembic.op.f("event_received_at_idx"),
        "event",
        ["received_at"],
        unique=False,
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
