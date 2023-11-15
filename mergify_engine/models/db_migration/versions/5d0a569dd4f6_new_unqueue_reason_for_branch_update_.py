"""New unqueue reason for branch update failure

Revision ID: 5d0a569dd4f6
Revises: 38f71c20a9da
Create Date: 2023-09-11 15:28:19.188214

"""

import alembic


revision = "5d0a569dd4f6"
down_revision = "38f71c20a9da"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.execute(
        "ALTER TYPE queuechecksabortcode ADD VALUE 'BRANCH_UPDATE_FAILED';",
    )
    alembic.op.execute(
        "ALTER TYPE queuechecksunqueuecode ADD VALUE 'BRANCH_UPDATE_FAILED';",
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
