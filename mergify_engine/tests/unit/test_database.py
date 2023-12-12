import asyncio
import difflib
import filecmp
import io
import os
import pathlib
import subprocess
from unittest import mock
import warnings

import alembic
import sqlalchemy
import sqlalchemy.ext.hybrid

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.config import types as config_types
from mergify_engine.models import github as gh_models
from mergify_engine.models import manage
from mergify_engine.tests import utils


def _alembic_thread(command: str, *args: str) -> str:
    asyncio.set_event_loop(asyncio.new_event_loop())
    config = manage.load_alembic_config()
    config.stdout = io.StringIO()

    APP_STATE = database.APP_STATE
    database.APP_STATE = None
    try:
        with mock.patch.object(settings, "LOG_STDOUT", command == "check"):
            getattr(alembic.command, command)(config, *args)
    finally:
        database.APP_STATE = APP_STATE
    return config.stdout.getvalue()


async def _run_alembic(command: str, *args: str) -> str:
    return await asyncio.to_thread(_alembic_thread, command, *args)


async def _run_migration_scripts(url: config_types.PostgresDSN) -> None:
    with mock.patch.object(settings, "DATABASE_URL", url):
        output = await _run_alembic("history")
        scripts_count = len(output.splitlines())
        for _ in range(scripts_count):
            await _run_alembic("upgrade", "+1")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sqlalchemy.exc.SAWarning)
            await _run_alembic("check")


async def test_migration(_setup_database: None, tmp_path: pathlib.Path) -> None:
    schema_dump_creation_path = tmp_path / "test_migration_create.sql"
    utils.dump_schema(settings.DATABASE_URL.path[1:], schema_dump_creation_path)

    if os.getenv("MIGRATED_DATA_DUMP") is None:
        url_migrate, url_migrate_without_db_name = utils.create_database_url(
            "test_migration_migrate",
        )
        await utils.create_database(
            url_migrate_without_db_name.geturl(),
            "test_migration_migrate",
        )
        await _run_migration_scripts(url_migrate)
        schema_dump_migration_path = tmp_path / "test_migration_migrate.sql"
        utils.dump_schema("test_migration_migrate", schema_dump_migration_path)
    else:
        schema_dump_migration_path = pathlib.Path(os.environ["MIGRATED_DATA_DUMP"])

    for _file in (schema_dump_creation_path, schema_dump_migration_path):
        # nosemgrep: python.lang.security.audit.subprocess-shell-true.subprocess-shell-true
        subprocess.run(
            "sed -i"
            " -e '/^--/d'"  # remove comments
            " -e '/^$/d'"  # remove empty lines
            " -e '/^CREATE EXTENSION/d' -e '/^COMMENT ON EXTENSION/d'"  # remove heroku extensions
            " -e 's/public\\.//g'"  # remove schema prefix
            f" {_file}",
            shell=True,
            check=True,
            timeout=10,
        )

    assert filecmp.cmp(
        schema_dump_creation_path,
        schema_dump_migration_path,
        shallow=False,
    ), filediff(schema_dump_creation_path, schema_dump_migration_path)


def filediff(path1: pathlib.Path, path2: pathlib.Path) -> str:
    with path1.open() as f1, path2.open() as f2:
        diff = difflib.unified_diff(
            f1.readlines(),
            f2.readlines(),
            path1.name,
            path2.name,
        )
        return "Database dump differences: \n" + "".join(diff)


async def test_one_head() -> None:
    output = await _run_alembic("heads")
    assert "(head)" in output
    heads = output.splitlines()
    assert (
        len(heads) == 1
    ), f"One head revision allowed, {len(heads)} found: {', '.join(heads)}"


async def test_get_or_create_on_conflict(_setup_database: None) -> None:
    account = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(1),
            "login": github_types.GitHubLogin("account"),
            "type": "User",
            "avatar_url": "",
        },
    )
    repo1 = github_types.GitHubRepository(
        {
            "id": github_types.GitHubRepositoryIdType(1),
            "name": github_types.GitHubRepositoryName("repo1"),
            "owner": account,
            "full_name": "account/repo1",
            "private": False,
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType("main"),
        },
    )
    repo2 = github_types.GitHubRepository(
        {
            "id": github_types.GitHubRepositoryIdType(2),
            "name": github_types.GitHubRepositoryName("repo2"),
            "owner": account,
            "full_name": "account/repo2",
            "private": False,
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType("main"),
        },
    )

    nb_try = 0
    # NOTE(Kontrolix): This whole mess is just here to simulate a session conflict for
    # test purpose, in reality the two sessions would have been opened in parallel
    async with database.create_session() as session1:
        session1.add(await gh_models.GitHubRepository.get_or_create(session1, repo1))

        async for attempt in database.tenacity_retry_on_pk_integrity_error(
            (gh_models.GitHubAccount, gh_models.GitHubRepository),
        ):
            with attempt:
                async with database.create_session() as session2:
                    nb_try += 1
                    session2.add(
                        await gh_models.GitHubRepository.get_or_create(session2, repo2),
                    )
                    await session1.commit()
                    await session2.commit()

    # To ensure that we try the session session twice
    assert nb_try == 2

    async with database.create_session() as session:
        repos = (
            await session.scalars(
                sqlalchemy.select(gh_models.GitHubRepository).order_by(
                    gh_models.GitHubRepository.id,
                ),
            )
        ).all()

    assert len(repos) == 2
    for repo_id, repo in enumerate(repos):
        assert repo_id + 1 == repo.id

    async with database.create_session() as session:
        accounts = (
            await session.scalars(sqlalchemy.select(gh_models.GitHubAccount))
        ).all()

    assert len(accounts) == 1
    assert accounts[0].id == 1
