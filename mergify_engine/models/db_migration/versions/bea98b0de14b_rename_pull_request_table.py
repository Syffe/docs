"""rename pull_request table

Revision ID: bea98b0de14b
Revises: 2df335ca04f5
Create Date: 2023-10-09 10:05:40.079124

"""
import alembic


revision = "bea98b0de14b"
down_revision = "2df335ca04f5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.rename_table("pull_request", "old_pull_request_for_ci_event_processing")
    alembic.op.drop_constraint(
        "pull_request_pkey",
        "old_pull_request_for_ci_event_processing",
    )
    alembic.op.create_primary_key(
        alembic.op.f("old_pull_request_for_ci_event_processing_pkey"),
        "old_pull_request_for_ci_event_processing",
        ["id"],
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
