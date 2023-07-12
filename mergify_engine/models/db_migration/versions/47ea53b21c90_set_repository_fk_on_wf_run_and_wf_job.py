"""set_repository_fk_on_wf_run_and_wf_job

Revision ID: 47ea53b21c90
Revises: 732a1510ec1d
Create Date: 2023-07-11 17:44:10.516853

"""
import alembic
import sqlalchemy


revision = "47ea53b21c90"
down_revision = "732a1510ec1d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column("repository_id", sqlalchemy.BigInteger(), nullable=True),
    )
    alembic.op.create_foreign_key(
        alembic.op.f("gha_workflow_job_repository_id_fkey"),
        "gha_workflow_job",
        "github_repository",
        ["repository_id"],
        ["id"],
    )
    alembic.op.add_column(
        "gha_workflow_run",
        sqlalchemy.Column("repository_id", sqlalchemy.BigInteger(), nullable=True),
    )
    alembic.op.drop_index(
        "gha_job_run_owner_id_repository_idx", table_name="gha_workflow_run"
    )
    alembic.op.create_index(
        "gha_workflow_run_owner_id_repository_id_idx",
        "gha_workflow_run",
        ["owner_id", "repository_id"],
        unique=False,
    )
    alembic.op.create_foreign_key(
        alembic.op.f("gha_workflow_run_repository_id_fkey"),
        "gha_workflow_run",
        "github_repository",
        ["repository_id"],
        ["id"],
    )
    alembic.op.drop_column("gha_workflow_run", "repository")


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
