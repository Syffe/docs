"""drop github_user sequence

Revision ID: 298d2a2d31b7
Revises: 67aef78efbee
Create Date: 2023-04-20 09:52:50.215340

"""

import alembic


revision = "298d2a2d31b7"
down_revision = "67aef78efbee"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.alter_column("github_user", "id", server_default=None)
    alembic.op.execute("DROP SEQUENCE github_user_id_seq;")


def downgrade() -> None:
    pass
