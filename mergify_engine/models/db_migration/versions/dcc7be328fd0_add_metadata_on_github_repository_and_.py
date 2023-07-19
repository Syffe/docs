"""Add metadata on GitHub repository and account

Revision ID: dcc7be328fd0
Revises: 9f3f9052a5cd
Create Date: 2023-07-12 15:33:19.775469

"""
import alembic
import sqlalchemy


revision = "dcc7be328fd0"
down_revision = "9689f24dfc76"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.add_column(
        "github_repository",
        sqlalchemy.Column("private", sqlalchemy.Boolean(), nullable=True),
    )
    alembic.op.add_column(
        "github_repository",
        sqlalchemy.Column("default_branch", sqlalchemy.Text(), nullable=True),
    )
    alembic.op.add_column(
        "github_repository",
        sqlalchemy.Column("full_name", sqlalchemy.Text(), nullable=True),
    )
    alembic.op.add_column(
        "github_repository",
        sqlalchemy.Column("archived", sqlalchemy.Boolean(), nullable=True),
    )

    alembic.op.execute(
        "CREATE TYPE githubaccounttype AS ENUM ('USER', 'ORGANIZATION', 'BOT')"
    )
    alembic.op.add_column(
        "github_account",
        sqlalchemy.Column(
            "type",
            sqlalchemy.Enum("USER", "ORGANIZATION", "BOT", name="githubaccounttype"),
            nullable=True,
        ),
    )


def downgrade() -> None:
    pass
