"""Split embedding and extract_metadata

Revision ID: 036151d4b7bb
Revises: b429ad516965
Create Date: 2023-11-27 15:44:33.609948

"""
import alembic
import sqlalchemy
from sqlalchemy.dialects import postgresql


revision = "036151d4b7bb"
down_revision = "b429ad516965"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.execute(
        "ALTER TYPE workflowjoblogstatus RENAME VALUE 'EMBEDDED' TO 'DOWNLOADED';",
    )

    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "log_downloading_attempts",
            sqlalchemy.Integer(),
            server_default="0",
            nullable=False,
            anonymizer_config=None,
        ),
    )
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "log_downloading_retry_after",
            sqlalchemy.DateTime(),
            nullable=True,
            anonymizer_config=None,
        ),
    )

    workflowjoblogembeddingstatus = postgresql.ENUM(
        "UNKNOWN",
        "ERROR",
        "EMBEDDED",
        name="workflowjoblogembeddingstatus",
        create_type=False,
    )
    workflowjoblogembeddingstatus.create(alembic.op.get_bind(), checkfirst=True)  # type: ignore [no-untyped-call]
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "log_embedding_status",
            workflowjoblogembeddingstatus,
            server_default="UNKNOWN",
            nullable=False,
            anonymizer_config=None,
        ),
    )
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "log_metadata_extracting_attempts",
            sqlalchemy.Integer(),
            server_default="0",
            nullable=False,
            anonymizer_config=None,
        ),
    )
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "log_metadata_extracting_retry_after",
            sqlalchemy.DateTime(),
            nullable=True,
            anonymizer_config=None,
        ),
    )

    workflowjoblogmetadataextractingstatus = postgresql.ENUM(
        "UNKNOWN",
        "ERROR",
        "EXTRACTED",
        name="workflowjoblogmetadataextractingstatus",
        create_type=False,
    )
    workflowjoblogmetadataextractingstatus.create(  # type: ignore [no-untyped-call]
        alembic.op.get_bind(),
        checkfirst=True,
    )
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "log_metadata_extracting_status",
            workflowjoblogmetadataextractingstatus,
            server_default="UNKNOWN",
            nullable=False,
            anonymizer_config=None,
        ),
    )

    alembic.op.drop_constraint(
        "gha_workflow_job_embedding_linked_columns_check",
        "gha_workflow_job",
    )

    alembic.op.create_check_constraint(
        "embedding_linked_columns",
        table_name="gha_workflow_job",
        condition="""
            (
                log_embedding_status = 'EMBEDDED' AND log_embedding IS NOT NULL
            ) OR (
                log_embedding_status != 'EMBEDDED' AND log_embedding IS NULL
            )
            """,
    )

    alembic.op.create_check_constraint(
        "metadata_extracting_retries",
        table_name="gha_workflow_job",
        condition="""
            log_metadata_extracting_attempts >= 0 AND (
                ( log_metadata_extracting_attempts = 0 AND log_metadata_extracting_retry_after IS NULL )
                OR ( log_metadata_extracting_attempts > 0 AND log_metadata_extracting_retry_after IS NOT NULL)
            )
            """,
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
