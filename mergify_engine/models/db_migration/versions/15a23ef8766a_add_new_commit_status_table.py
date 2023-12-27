"""add new commit_status table

Revision ID: 15a23ef8766a
Revises: 5622d4007f90
Create Date: 2023-12-26 16:47:52.355044

"""
import alembic
import sqlalchemy


revision = "15a23ef8766a"
down_revision = "5622d4007f90"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    alembic.op.create_table(
        "commit_status",
        sqlalchemy.Column(
            "id",
            sqlalchemy.BigInteger(),
            autoincrement=False,
            nullable=False,
            anonymizer_config=None,
        ),
        sqlalchemy.Column(
            "context",
            sqlalchemy.Text(),
            nullable=False,
            anonymizer_config="anon.lorem_ipsum( characters := 7 )",
        ),
        sqlalchemy.Column(
            "state",
            sqlalchemy.Text(),
            nullable=False,
            anonymizer_config="anon.lorem_ipsum( characters := 7 )",
        ),
        sqlalchemy.Column(
            "description",
            sqlalchemy.Text(),
            nullable=True,
            anonymizer_config="anon.lorem_ipsum( words := 7 )",
        ),
        sqlalchemy.Column(
            "target_url",
            sqlalchemy.Text(),
            nullable=True,
            anonymizer_config="anon.lorem_ipsum( characters := 20 )",
        ),
        sqlalchemy.Column(
            "avatar_url",
            sqlalchemy.Text(),
            nullable=True,
            anonymizer_config="anon.lorem_ipsum( characters := 20 )",
        ),
        sqlalchemy.Column(
            "created_at",
            sqlalchemy.DateTime(timezone=True),
            nullable=False,
            anonymizer_config="anon.dnoise(created_at, ''1 hour'')",
        ),
        sqlalchemy.Column(
            "updated_at",
            sqlalchemy.DateTime(timezone=True),
            nullable=False,
            anonymizer_config="anon.dnoise(updated_at, ''1 hour'')",
        ),
        sqlalchemy.Column(
            "sha",
            sqlalchemy.Text(),
            nullable=False,
            anonymizer_config="anon.lorem_ipsum( characters := 20 )",
        ),
        sqlalchemy.Column(
            "name",
            sqlalchemy.Text(),
            nullable=False,
            anonymizer_config="anon.lorem_ipsum( characters := 20 )",
        ),
        sqlalchemy.Column(
            "repo_id",
            sqlalchemy.BigInteger(),
            nullable=False,
            anonymizer_config=None,
        ),
        sqlalchemy.ForeignKeyConstraint(
            ["repo_id"],
            ["github_repository.id"],
            name=alembic.op.f("commit_status_repo_id_fkey"),
        ),
        sqlalchemy.PrimaryKeyConstraint("id", name=alembic.op.f("commit_status_pkey")),
    )
    alembic.op.create_index(
        alembic.op.f("commit_status_context_idx"),
        "commit_status",
        ["context"],
        unique=False,
    )
    alembic.op.create_index(
        alembic.op.f("commit_status_sha_idx"),
        "commit_status",
        ["sha"],
        unique=False,
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
