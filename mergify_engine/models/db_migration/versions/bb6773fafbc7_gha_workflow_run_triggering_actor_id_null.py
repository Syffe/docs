"""

Allow gha_workflow_run.triggering_actor_id to be NULL

Revision ID: bb6773fafbc7
Revises: b01cb243b4d1
Create Date: 2023-07-06 23:27:41.182217

"""
import alembic


revision = "bb6773fafbc7"
down_revision = "0cd24c08a6ad"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.alter_column(
        "gha_workflow_run",
        "triggering_actor_id",
        nullable=True,
    )


def downgrade() -> None:
    pass
