"""Allow committer to be null in pull request commits

Revision ID: 5622d4007f90
Revises: 986da9271dd1
Create Date: 2023-12-22 18:19:44.362660

"""
import alembic
import sqlalchemy


revision = "5622d4007f90"
down_revision = "3780489eff79"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.alter_column(
        "pull_request_commit",
        "committer_id",
        existing_type=sqlalchemy.String,
        nullable=True,
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
