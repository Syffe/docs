"""Set base_repository_id not null

Revision ID: bfb58ea66caf
Revises: c3263ca8c1c0
Create Date: 2023-12-07 16:05:15.528259

"""
import alembic
import sqlalchemy


revision = "bfb58ea66caf"
down_revision = "c3263ca8c1c0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.alter_column(
        "pull_request",
        "base_repository_id",
        existing_type=sqlalchemy.BIGINT(),
        nullable=False,
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
