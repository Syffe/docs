from unittest import mock

import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.config import types
from mergify_engine.rules.config import mergify as mergify_conf


async def load_mergify_config(content: str) -> mergify_conf.MergifyConfig:
    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content=content,
    )

    return await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)


def create_database_url(db_name: str) -> tuple[types.PostgresDSN, types.PostgresDSN]:
    mocked_url = settings.DATABASE_URL._replace(path=f"/{db_name}")
    mocked_url_without_db_name = settings.DATABASE_URL._replace(path="")
    return mocked_url, mocked_url_without_db_name


async def create_database(db_url: str, db_name: str) -> None:
    engine = sqlalchemy.ext.asyncio.create_async_engine(db_url)
    try:
        engine_no_transaction = engine.execution_options(isolation_level="AUTOCOMMIT")
        async with engine_no_transaction.connect() as conn:
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            await conn.execute(sqlalchemy.text(f"DROP DATABASE IF EXISTS {db_name}"))
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            await conn.execute(sqlalchemy.text(f"CREATE DATABASE {db_name}"))
    finally:
        await engine.dispose()
