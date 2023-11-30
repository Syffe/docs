"""Application key UUID

Revision ID: bf59bf717b18
Revises: 036151d4b7bb
Create Date: 2023-11-16 12:35:37.638439

"""
import alembic
import sqlalchemy


revision = "bf59bf717b18"
down_revision = "036151d4b7bb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.alter_column("application", "id", new_column_name="id_")
    alembic.op.drop_constraint("application_pkey", "application")
    alembic.op.add_column(
        "application",
        sqlalchemy.Column(
            "id",
            sqlalchemy.Uuid(),
            server_default=sqlalchemy.text("gen_random_uuid()"),
            nullable=False,
            anonymizer_config=None,
        ),
    )
    alembic.op.create_primary_key("application_pkey", "application", ["id"])
    alembic.op.drop_column("application", "id_")


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
