"""Renamed column 'name' to 'name_without_matrix' in WorkflowJob model

Revision ID: a72a0edc7d19
Revises: 5e8e1df744a6
Create Date: 2023-10-18 16:49:49.369323

"""
import alembic


revision = "a72a0edc7d19"
down_revision = "5e8e1df744a6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.alter_column(
        "gha_workflow_job",
        "name",
        new_column_name="name_without_matrix",
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
