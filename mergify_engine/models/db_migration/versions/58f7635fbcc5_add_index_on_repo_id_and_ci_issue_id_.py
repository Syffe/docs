"""Add index on repo_id and ci_issue_id for wj_job

Revision ID: 58f7635fbcc5
Revises: 3cf7ed91b24f
Create Date: 2023-11-07 16:49:15.828697

"""
import alembic


revision = "58f7635fbcc5"
down_revision = "3cf7ed91b24f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.create_index(
        alembic.op.f("gha_workflow_job_ci_issue_id_idx"),
        "gha_workflow_job",
        ["ci_issue_id"],
        unique=False,
    )

    alembic.op.create_index(
        "idx_gha_workflow_job_ci_issue_id_in_repo",
        "gha_workflow_job",
        ["repository_id", "ci_issue_id"],
        unique=False,
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
