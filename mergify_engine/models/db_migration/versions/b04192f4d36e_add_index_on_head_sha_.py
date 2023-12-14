"""Add index on head_sha fields

Revision ID: b04192f4d36e
Revises: ec04308a2376
Create Date: 2023-12-14 09:58:09.936190

"""
import alembic


revision = "b04192f4d36e"
down_revision = "ec04308a2376"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.create_index(
        alembic.op.f("gha_workflow_job_head_sha_idx"),
        "gha_workflow_job",
        ["head_sha"],
        unique=False,
    )
    alembic.op.create_index(
        alembic.op.f("pull_request_head_sha_history_head_sha_idx"),
        "pull_request_head_sha_history",
        ["head_sha"],
        unique=False,
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
