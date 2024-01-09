"""remove unused table

Revision ID: 5430e2f0c9c6
Revises: b0e33391e95f
Create Date: 2024-01-09 11:51:46.253477

"""
import alembic


revision = "5430e2f0c9c6"
down_revision = "b0e33391e95f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    alembic.op.drop_table("old_pull_request_for_ci_event_processing")
    # ### end Alembic commands ###


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
