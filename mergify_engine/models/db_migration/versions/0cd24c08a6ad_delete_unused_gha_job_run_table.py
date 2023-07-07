"""Delete unused gha_job_run table

Revision ID: 0cd24c08a6ad
Revises: 75e4ba139327
Create Date: 2023-07-05 16:32:06.738045

"""

import alembic


revision = "0cd24c08a6ad"
down_revision = "75e4ba139327"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.drop_table("jt_gha_job_run_pull_request")
    alembic.op.drop_table("gha_job_run")
    alembic.op.execute("DROP TYPE jobrunoperatingsystem")


def downgrade() -> None:
    pass
