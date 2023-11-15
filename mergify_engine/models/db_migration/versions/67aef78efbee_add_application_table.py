"""Add application table

Revision ID: 67aef78efbee
Revises: 787e61eabca7
Create Date: 2023-04-17 17:17:32.744679

"""
import alembic
import sqlalchemy
from sqlalchemy.dialects import postgresql


revision = "67aef78efbee"
down_revision = "787e61eabca7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.create_table(
        "application",
        sqlalchemy.Column(
            "id",
            sqlalchemy.Integer(),
            autoincrement=True,
            nullable=False,
        ),
        sqlalchemy.Column("name", sqlalchemy.String(length=255), nullable=False),
        sqlalchemy.Column(
            "api_access_key",
            sqlalchemy.String(length=255),
            nullable=False,
        ),
        sqlalchemy.Column(
            "api_secret_key",
            postgresql.BYTEA,
            nullable=False,
        ),
        sqlalchemy.Column("github_account_id", sqlalchemy.BigInteger(), nullable=False),
        sqlalchemy.Column(
            "created_by_github_user_id",
            sqlalchemy.Integer(),
            nullable=True,
        ),
        sqlalchemy.Column(
            "created_at",
            sqlalchemy.DateTime(),
            server_default=sqlalchemy.text("now()"),
            nullable=False,
        ),
        sqlalchemy.ForeignKeyConstraint(
            ["created_by_github_user_id"],
            ["github_user.id"],
            name=alembic.op.f("application_created_by_github_user_id_fkey"),
        ),
        sqlalchemy.ForeignKeyConstraint(
            ["github_account_id"],
            ["github_account.id"],
            name=alembic.op.f("application_github_account_id_fkey"),
        ),
        sqlalchemy.PrimaryKeyConstraint("id", name=alembic.op.f("application_pkey")),
    )
    alembic.op.create_index(
        alembic.op.f("application_github_account_id_idx"),
        "application",
        ["github_account_id"],
        unique=False,
    )
