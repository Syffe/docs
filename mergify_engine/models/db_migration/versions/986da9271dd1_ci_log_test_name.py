"""Add CI run test name

Revision ID: 986da9271dd1
Revises: 3780489eff79
Create Date: 2023-12-22 10:12:02.183831

"""
import alembic
import sqlalchemy


revision = "986da9271dd1"
down_revision = "15a23ef8766a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.add_column(
        "gha_workflow_job_log_metadata",
        sqlalchemy.Column("test_name", sqlalchemy.String),
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
