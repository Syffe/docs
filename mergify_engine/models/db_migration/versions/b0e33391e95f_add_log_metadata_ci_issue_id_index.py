"""add log_metadata.ci_issue_id index

Revision ID: b0e33391e95f
Revises: fcf5267c5e18
Create Date: 2024-01-08 14:17:18.209104

"""
import alembic


revision = "b0e33391e95f"
down_revision = "fcf5267c5e18"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.create_index(
        alembic.op.f("gha_workflow_job_log_metadata_ci_issue_id_idx"),
        "gha_workflow_job_log_metadata",
        ["ci_issue_id"],
        unique=False,
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
