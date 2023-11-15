"""Rename github_repository.last_dump_at

Revision ID: a875052e527c
Revises: 01294c4668ae
Create Date: 2023-06-08 10:09:47.803641

"""

import alembic


revision = "a875052e527c"
down_revision = "01294c4668ae"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.alter_column(
        "github_repository",
        "last_dump_at",
        new_column_name="last_download_at",
    )


def downgrade() -> None:
    pass
