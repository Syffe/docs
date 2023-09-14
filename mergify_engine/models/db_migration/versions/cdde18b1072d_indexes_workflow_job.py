"""indexes_workflow_job

Revision ID: cdde18b1072d
Revises: 5d0a569dd4f6
Create Date: 2023-09-13 17:32:19.328196

"""
import alembic


revision = "cdde18b1072d"
down_revision = "5d0a569dd4f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.create_index(
        "idx_gha_workflow_job_conclusion_failure_repository_id",
        "gha_workflow_job",
        ["conclusion", "repository_id"],
        unique=False,
        postgresql_where="conclusion = 'FAILURE'::workflowjobconclusion",
    )
    alembic.op.create_index(
        "idx_gha_workflow_job_rerun_compound",
        "gha_workflow_job",
        ["run_attempt", "repository_id", "name", "workflow_run_id", "conclusion"],
        unique=False,
        postgresql_where="conclusion = 'SUCCESS'::workflowjobconclusion",
    )
    alembic.op.create_index(
        alembic.op.f("gha_workflow_job_log_neighbours_job_id_idx"),
        "gha_workflow_job_log_neighbours",
        ["job_id"],
        unique=False,
    )
    alembic.op.create_index(
        alembic.op.f("gha_workflow_job_log_neighbours_neighbour_job_id_idx"),
        "gha_workflow_job_log_neighbours",
        ["neighbour_job_id"],
        unique=False,
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
