"""CI issue status

Revision ID: 915eac333393
Revises: b429ad516965
Create Date: 2023-11-29 13:24:56.422729

"""
import alembic
import sqlalchemy
from sqlalchemy.dialects import postgresql


revision = "915eac333393"
down_revision = "b429ad516965"
branch_labels = None
depends_on = None


def upgrade() -> None:
    ciissuestatus = postgresql.ENUM(
        "UNRESOLVED",
        "RESOLVED",
        name="ciissuestatus",
        create_type=False,
    )
    ciissuestatus.create(alembic.op.get_bind(), checkfirst=True)  # type: ignore [no-untyped-call]

    alembic.op.add_column(
        "ci_issue",
        sqlalchemy.Column(
            "status",
            ciissuestatus,
            server_default="UNRESOLVED",
            nullable=False,
            anonymizer_config="anon.random_in_enum(status)",
        ),
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
