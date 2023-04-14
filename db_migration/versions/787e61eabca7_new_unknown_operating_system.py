"""New unknown operating system

Revision ID: 787e61eabca7
Revises: f22dca1d06a8
Create Date: 2023-04-12 15:10:04.068167

"""
import alembic


revision = "787e61eabca7"
down_revision = "f22dca1d06a8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    alembic.op.execute("ALTER TYPE jobrunoperatingsystem ADD VALUE 'UNKNOWN';")
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
