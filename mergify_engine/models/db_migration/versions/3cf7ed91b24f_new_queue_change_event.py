"""New queue change event

Revision ID: 3cf7ed91b24f
Revises: 81392e595d98
Create Date: 2023-10-26 16:42:54.583528

"""

import alembic
import sqlalchemy


revision = "3cf7ed91b24f"
down_revision = "81392e595d98"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.execute("ALTER TYPE eventtype ADD VALUE 'action.queue.change'")

    alembic.op.create_table(
        "event_action_queue_change",
        sqlalchemy.Column("id", sqlalchemy.BigInteger(), nullable=False),
        sqlalchemy.Column("queue_name", sqlalchemy.Text(), nullable=False),
        sqlalchemy.Column("partition_name", sqlalchemy.Text(), nullable=True),
        sqlalchemy.Column("size", sqlalchemy.Integer(), nullable=False),
        sqlalchemy.Column("running_checks", sqlalchemy.Integer(), nullable=False),
        sqlalchemy.ForeignKeyConstraint(
            ["id"], ["event.id"], name=alembic.op.f("event_action_queue_change_id_fkey")
        ),
        sqlalchemy.PrimaryKeyConstraint(
            "id", name=alembic.op.f("event_action_queue_change_pkey")
        ),
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
