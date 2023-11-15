"""Rename GitHub Actions enums

Revision ID: 9689f24dfc76
Revises: 5ec6e7b467ed
Create Date: 2023-07-13 15:02:15.769420

"""

import alembic


revision = "9689f24dfc76"
down_revision = "5ec6e7b467ed"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.execute("alter type jobrunconclusion rename to workflowjobconclusion")
    alembic.op.execute(
        "alter type jobruntriggerevent rename to workflowruntriggerevent",
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
