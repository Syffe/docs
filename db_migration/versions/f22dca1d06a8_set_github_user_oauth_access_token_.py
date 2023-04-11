"""Set github_user.oauth_access_token length

Revision ID: f22dca1d06a8
Revises: 49d540f8edc6
Create Date: 2023-03-31 10:53:21.627853

"""
import alembic
import sqlalchemy


revision = "f22dca1d06a8"
down_revision = "49d540f8edc6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.alter_column(
        "github_user", "oauth_access_token", type_=sqlalchemy.String(length=512)
    )


def downgrade() -> None:
    pass
