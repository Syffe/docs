"""rename_log_extract

Revision ID: 56e12b9a1fb5
Revises: e03fc10bcaf2
Create Date: 2023-12-08 19:31:58.117767

"""
import alembic


revision = "56e12b9a1fb5"
down_revision = "9c49881b1fcb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.alter_column(
        "gha_workflow_job",
        "embedded_log",
        new_column_name="log_extract",
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
