"""Event model action backport

Revision ID: 6fac7379497e
Revises: 52f21f4b4e6b
Create Date: 2023-07-05 15:37:45.391887

"""
import alembic
import sqlalchemy


revision = "6fac7379497e"
down_revision = "c05edaa912c7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    alembic.op.create_table(
        "event_action_backport",
        sqlalchemy.Column("id", sqlalchemy.BigInteger(), nullable=False),
        sqlalchemy.Column("to", sqlalchemy.Text(), nullable=False),
        sqlalchemy.Column("pull_request_number", sqlalchemy.Integer(), nullable=False),
        sqlalchemy.Column("conflicts", sqlalchemy.Boolean(), nullable=False),
        sqlalchemy.ForeignKeyConstraint(
            ["id"], ["event.id"], name=alembic.op.f("event_action_backport_id_fkey")
        ),
        sqlalchemy.PrimaryKeyConstraint(
            "id", name=alembic.op.f("event_action_backport_pkey")
        ),
    )
    # ### end Alembic commands ###
