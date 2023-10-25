"""Drop useless embedded_log_error_title

Revision ID: 9662e4efa6ed
Revises: 49425c551193
Create Date: 2023-10-20 17:14:30.068041

"""
import alembic


revision = "9662e4efa6ed"
down_revision = "49425c551193"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.drop_column("gha_workflow_job", "embedded_log_error_title")


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
