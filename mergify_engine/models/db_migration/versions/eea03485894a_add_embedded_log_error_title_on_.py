"""Add embedded_log_error_title on workflow_job

Revision ID: eea03485894a
Revises: 555eb4b26868
Create Date: 2023-08-30 17:26:37.601941

"""
import alembic
import sqlalchemy


revision = "eea03485894a"
down_revision = "555eb4b26868"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "embedded_log_error_title",
            sqlalchemy.String(),
            nullable=True,
        ),
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
