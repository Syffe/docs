"""Delete column github_repository.last_download_at

Revision ID: 75e4ba139327
Revises: 85830086e719
Create Date: 2023-07-05 16:24:12.991262

"""

import alembic


revision = "75e4ba139327"
down_revision = "52f21f4b4e6b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.drop_column("github_repository", "last_download_at")


def downgrade() -> None:
    pass
