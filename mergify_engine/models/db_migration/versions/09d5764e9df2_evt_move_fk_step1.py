"""Evt checks move fk to speculative_check_pull_request

Revision ID: 09d5764e9df2
Revises: 00303898d2c1
Create Date: 2023-11-14 13:46:37.413606

"""
import alembic
import sqlalchemy


revision = "09d5764e9df2"
down_revision = "00303898d2c1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    alembic.op.add_column(
        "speculative_check_pull_request",
        sqlalchemy.Column(
            "event_id",
            sqlalchemy.Integer(),
            nullable=True,
            anonymizer_config=None,
        ),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
