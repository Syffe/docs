"""Add event action label

Revision ID: 7b2a0192424f
Revises: 616124037cdd
Create Date: 2023-07-10 10:51:36.281744

"""
import alembic
import sqlalchemy


revision = "7b2a0192424f"
down_revision = "616124037cdd"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    alembic.op.create_table(
        "event_action_label",
        sqlalchemy.Column("id", sqlalchemy.BigInteger(), nullable=False),
        sqlalchemy.Column(
            "added",
            sqlalchemy.ARRAY(sqlalchemy.Text(), dimensions=1),
            nullable=False,
        ),
        sqlalchemy.Column(
            "removed",
            sqlalchemy.ARRAY(sqlalchemy.Text(), dimensions=1),
            nullable=False,
        ),
        sqlalchemy.ForeignKeyConstraint(
            ["id"],
            ["event.id"],
            name=alembic.op.f("event_action_label_id_fkey"),
        ),
        sqlalchemy.PrimaryKeyConstraint(
            "id",
            name=alembic.op.f("event_action_label_pkey"),
        ),
    )
    # ### end Alembic commands ###
