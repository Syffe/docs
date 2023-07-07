"""Event model action delete_head_branch

Revision ID: bee9e2dfda9d
Revises: 6fac7379497e
Create Date: 2023-07-05 17:10:23.170110

"""
import alembic
import sqlalchemy


revision = "bee9e2dfda9d"
down_revision = "bb6773fafbc7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    alembic.op.create_table(
        "event_action_delete_head_branch",
        sqlalchemy.Column("id", sqlalchemy.BigInteger(), nullable=False),
        sqlalchemy.Column("branch", sqlalchemy.Text(), nullable=False),
        sqlalchemy.ForeignKeyConstraint(
            ["id"],
            ["event.id"],
            name=alembic.op.f("event_action_delete_head_branch_id_fkey"),
        ),
        sqlalchemy.PrimaryKeyConstraint(
            "id", name=alembic.op.f("event_action_delete_head_branch_pkey")
        ),
    )
    # ### end Alembic commands ###
