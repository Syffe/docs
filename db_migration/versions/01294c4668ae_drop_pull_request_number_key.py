"""Drop pull_request_number_key

Revision ID: 01294c4668ae
Revises: 7a5673ee5829
Create Date: 2023-05-31 16:45:17.208904

"""

import alembic


revision = "01294c4668ae"
down_revision = "7a5673ee5829"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.drop_constraint("pull_request_number_key", "pull_request")


def downgrade() -> None:
    pass
