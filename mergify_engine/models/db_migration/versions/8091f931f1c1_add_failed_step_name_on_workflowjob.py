"""Add failed_step_name on workflowJob

Revision ID: 8091f931f1c1
Revises: c20ce2fbc65c
Create Date: 2023-08-23 16:00:12.268809

"""
import alembic
import sqlalchemy


revision = "8091f931f1c1"
down_revision = "c20ce2fbc65c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "failed_step_name",
            sqlalchemy.String(),
            nullable=True,
            anonymizer_config=None,
        ),
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
