"""github_repository owner_id+name index

Revision ID: 0305f7aaf9cc
Revises: dcc7be328fd0
Create Date: 2023-07-19 10:46:11.412850

"""
import alembic


revision = "0305f7aaf9cc"
down_revision = "dcc7be328fd0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.create_index(
        "github_repository_owner_id_name_idx",
        "github_repository",
        ["owner_id", "name"],
        unique=False,
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
