"""Add base_repository_id on pull_request

Revision ID: e03fc10bcaf2
Revises: f9593e3ec0da
Create Date: 2023-12-06 10:43:07.516268

"""
import alembic
import sqlalchemy


revision = "e03fc10bcaf2"
down_revision = "f9593e3ec0da"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.add_column(
        "pull_request",
        sqlalchemy.Column(
            "base_repository_id",
            sqlalchemy.BigInteger(),
            nullable=True,
            anonymizer_config=None,
        ),
    )
    alembic.op.create_foreign_key(
        alembic.op.f("pull_request_base_repository_id_fkey"),
        "pull_request",
        "github_repository",
        ["base_repository_id"],
        ["id"],
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
