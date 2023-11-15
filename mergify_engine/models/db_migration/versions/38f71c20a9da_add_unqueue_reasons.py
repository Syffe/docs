"""Add unqueue reasons

Revision ID: 38f71c20a9da
Revises: 8a897f49bb06
Create Date: 2023-09-08 11:43:46.239614

"""

import alembic


revision = "38f71c20a9da"
down_revision = "8a897f49bb06"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.execute(
        "ALTER TYPE queuechecksabortcode ADD VALUE 'CONFLICT_WITH_BASE_BRANCH';",
    )
    alembic.op.execute(
        "ALTER TYPE queuechecksabortcode ADD VALUE 'CONFLICT_WITH_PULL_AHEAD';",
    )
    alembic.op.execute(
        "ALTER TYPE queuechecksunqueuecode ADD VALUE 'CONFLICT_WITH_BASE_BRANCH';",
    )
    alembic.op.execute(
        "ALTER TYPE queuechecksunqueuecode ADD VALUE 'CONFLICT_WITH_PULL_AHEAD';",
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
