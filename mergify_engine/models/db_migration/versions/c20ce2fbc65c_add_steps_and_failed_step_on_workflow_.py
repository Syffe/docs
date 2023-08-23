"""Add steps and failed_step_number on workflow_job

Revision ID: c20ce2fbc65c
Revises: 9e62d202d44f
Create Date: 2023-08-22 16:49:18.137277

"""
import alembic
import sqlalchemy


revision = "c20ce2fbc65c"
down_revision = "9e62d202d44f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "steps", sqlalchemy.JSON(), nullable=True, anonymizer_config=None
        ),
    )
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "failed_step_number",
            sqlalchemy.Integer(),
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
