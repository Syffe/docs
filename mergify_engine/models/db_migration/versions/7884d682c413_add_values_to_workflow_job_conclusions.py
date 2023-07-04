"""Add values to workflow_job conclusions

Revision ID: 7884d682c413
Revises: 176199dd35b1
Create Date: 2023-07-04 10:05:31.145213

"""

import alembic


revision = "7884d682c413"
down_revision = "176199dd35b1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.execute("ALTER TYPE jobrunconclusion ADD VALUE 'NEUTRAL'")
    alembic.op.execute("ALTER TYPE jobrunconclusion ADD VALUE 'TIMED_OUT'")
    alembic.op.execute("ALTER TYPE jobrunconclusion ADD VALUE 'ACTION_REQUIRED'")


def downgrade() -> None:
    pass
