"""Remove code relative to WorkflowJobLogNeighbours

Revision ID: 00303898d2c1
Revises: b1059b7235df
Create Date: 2023-11-09 11:48:49.321693

"""
import alembic


revision = "00303898d2c1"
down_revision = "b1059b7235df"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.drop_index(
        "gha_workflow_job_log_neighbours_job_id_idx",
        table_name="gha_workflow_job_log_neighbours",
    )
    alembic.op.drop_index(
        "gha_workflow_job_log_neighbours_neighbour_job_id_idx",
        table_name="gha_workflow_job_log_neighbours",
    )
    alembic.op.drop_table("gha_workflow_job_log_neighbours")
    alembic.op.drop_column("gha_workflow_job", "neighbours_computed_at")


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
