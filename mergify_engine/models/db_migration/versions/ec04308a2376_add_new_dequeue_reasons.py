"""Add new dequeue reasons

Revision ID: ec04308a2376
Revises: 56e12b9a1fb5
Create Date: 2023-12-12 21:36:55.440565

"""
import alembic


revision = "ec04308a2376"
down_revision = "bfb58ea66caf"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.execute(
        "ALTER TYPE queuechecksabortcode ADD VALUE 'DRAFT_PULL_REQUEST_CHANGED';",
    )
    alembic.op.execute(
        "ALTER TYPE queuechecksabortcode ADD VALUE 'PULL_REQUEST_UPDATED';",
    )
    alembic.op.execute("ALTER TYPE queuechecksabortcode ADD VALUE 'MERGE_QUEUE_RESET';")


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
