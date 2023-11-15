"""First migration script

Revision ID: 49d540f8edc6
Revises:
Create Date: 2023-03-27 15:48:51.991364

"""
import alembic
import sqlalchemy


revision = "49d540f8edc6"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sqlalchemy.inspect(alembic.op.get_bind())
    if not inspector.has_table("github_user"):
        # ### commands auto generated by Alembic - please adjust! ###
        alembic.op.create_table(
            "github_user",
            sqlalchemy.Column("id", sqlalchemy.Integer(), nullable=False),
            sqlalchemy.Column("login", sqlalchemy.String(length=255), nullable=False),
            sqlalchemy.Column(
                "oauth_access_token",
                sqlalchemy.String(),
                nullable=False,
            ),
            sqlalchemy.PrimaryKeyConstraint("id"),
        )
        # ### end Alembic commands ###


def downgrade() -> None:
    pass
