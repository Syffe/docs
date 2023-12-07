"""Add cascade ondelete on WorkfloJobLobMetadata and add index on WorkflowJob completed_at

Revision ID: f9593e3ec0da
Revises: da41815ebbbe
Create Date: 2023-12-07 10:25:38.537311

"""
import alembic


revision = "f9593e3ec0da"
down_revision = "da41815ebbbe"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    alembic.op.create_index(
        alembic.op.f("gha_workflow_job_completed_at_idx"),
        "gha_workflow_job",
        ["completed_at"],
        unique=False,
    )
    alembic.op.drop_constraint(
        "gha_workflow_job_log_metadata_workflow_job_id_fkey",
        "gha_workflow_job_log_metadata",
        type_="foreignkey",
    )
    alembic.op.create_foreign_key(
        alembic.op.f("gha_workflow_job_log_metadata_workflow_job_id_fkey"),
        "gha_workflow_job_log_metadata",
        "gha_workflow_job",
        ["workflow_job_id"],
        ["id"],
        ondelete="CASCADE",
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
