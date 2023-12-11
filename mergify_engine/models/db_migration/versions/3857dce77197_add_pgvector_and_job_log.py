"""Add pgvector and job log

Revision ID: 3857dce77197
Revises: b01cb243b4d1
Create Date: 2023-07-05 14:50:15.235034

"""
import alembic
from alembic_utils.pg_extension import PGExtension
from pgvector.sqlalchemy import Vector  # type: ignore[import-untyped]
import sqlalchemy


revision = "3857dce77197"
down_revision = "7b2a0192424f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    public_vector = PGExtension(schema="public", signature="vector")
    alembic.op.create_entity(public_vector)  # type: ignore[attr-defined]

    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column("log_embedding", Vector(1536)),
    )


def downgrade() -> None:
    pass
