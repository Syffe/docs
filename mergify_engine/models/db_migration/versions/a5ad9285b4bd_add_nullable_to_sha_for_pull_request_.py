"""add nullable to sha for pull_request_file

Revision ID: a5ad9285b4bd
Revises: 61ec48864943
Create Date: 2024-01-11 17:56:04.450481

"""
import alembic
import sqlalchemy


revision = "a5ad9285b4bd"
down_revision = "61ec48864943"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.alter_column(
        "pull_request_file",
        "sha",
        existing_type=sqlalchemy.TEXT(),
        nullable=True,
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
