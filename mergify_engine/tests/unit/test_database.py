import asyncio
import difflib
import filecmp
import io
import os
import pathlib
from unittest import mock
import warnings

import alembic
import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql
import sqlalchemy.ext.hybrid

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.config import types as config_types
from mergify_engine.models import github as gh_models
from mergify_engine.models import manage
from mergify_engine.models.db_migration.versions import (
    c3263ca8c1c0_update_base_repository_id_on_pull_ as c3263ca8c1c0,
)
from mergify_engine.tests import conftest
from mergify_engine.tests import utils
from mergify_engine.tests.db_populator import DbPopulator


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
        with _file.open("r") as f:
            content = f.readlines()

        with _file.open("w") as f:
            for line in content:
                if not line.strip() or line.startswith(
                    ("--", "CREATE EXTENSION", "COMMENT ON EXTENSION"),
                ):
                    continue

                line.replace("public.", "")

            f.write(line)

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


@pytest.mark.parametrize("mergifyengine_saas_mode", (True, False))
async def test_migration_script_c3263ca8c1c0(
    mergifyengine_saas_mode: bool,
    _reset_database_state: None,
    _reset_dbpopulator: None,
    _mock_gh_pull_request_commits_insert_in_pg: None,
    _mock_gh_pull_request_files_insert_in_pg: None,
) -> None:
    async def prepare_data() -> None:
        async with database.create_session() as session:
            # NOTE(Kontrolix): Using DbPopulator is a potential risk of failure for this
            # test in the future since this test will upgrade the database to
            # a certain point in the migration and then use DbPopulator that relies on
            # the current orm schema. It's known and assumed because it allows us to avoid
            # a lot of code in this test to insert pull requests in db. If the test fails
            # in the future, it's probably safe to remove it, since the migration script
            # is a one-shot and would have been troughly tested. This can only be done
            # here exceptionnaly because this part is not yet deployd on-premise.
            await DbPopulator.load(session, {"TestGhaFailedJobsPullRequestsDataset"})

            await session.execute(
                sqlalchemy.update(c3263ca8c1c0.pull_request).values(
                    base_repository_id=None,
                ),
            )
            await session.execute(
                sqlalchemy.update(c3263ca8c1c0.pull_request).values(
                    base=sqlalchemy.func.jsonb_set(
                        c3263ca8c1c0.pull_request.c.base,
                        ["repo", "owner", "id"],
                        sqlalchemy.cast(999, postgresql.JSONB),
                    ),
                ),
            )
            await session.execute(
                sqlalchemy.update(c3263ca8c1c0.pull_request).values(
                    base=sqlalchemy.func.jsonb_set(
                        c3263ca8c1c0.pull_request.c.base,
                        ["repo", "id"],
                        sqlalchemy.cast(888, postgresql.JSONB),
                    ),
                ),
            )

            await session.commit()

    async def assert_data(
        repo_exists: bool,
        owner_exists: bool,
        nb_pr_to_update: int,
    ) -> None:
        async with database.create_session() as session:
            assert (
                await session.execute(
                    sqlalchemy.exists()
                    .where(
                        c3263ca8c1c0.github_repository.c.id == 888,
                    )
                    .select(),
                )
            ).scalar() == repo_exists

            assert (
                await session.execute(
                    sqlalchemy.exists()
                    .where(
                        c3263ca8c1c0.github_account.c.id == 999,
                    )
                    .select(),
                )
            ).scalar() == owner_exists

            assert (
                await session.execute(
                    sqlalchemy.select(sqlalchemy.func.count()).where(
                        c3263ca8c1c0.pull_request.c.base_repository_id.is_(None),
                    ),
                )
            ).scalar() == nb_pr_to_update

    db_name = "test_update_base_repository_id_on_pull_request"

    url_migrate, url_migrate_without_db_name = utils.create_database_url(db_name)

    await utils.create_database(url_migrate_without_db_name.geturl(), db_name)

    with mock.patch.multiple(
        settings,
        DATABASE_URL=url_migrate,
        SAAS_MODE=mergifyengine_saas_mode,
    ):
        await _run_alembic("upgrade", c3263ca8c1c0.down_revision)
        database.init_sqlalchemy("migration_script")

        await prepare_data()
        await assert_data(repo_exists=False, owner_exists=False, nb_pr_to_update=4)
        await _run_alembic("upgrade", c3263ca8c1c0.revision)
        if mergifyengine_saas_mode:
            # NOTE(Kontrolix): In manual mode we call `init_sqlalchemy` so we have
            # reset database state first
            await conftest.reset_database_state()
            await c3263ca8c1c0.manual_run()

        await assert_data(repo_exists=True, owner_exists=True, nb_pr_to_update=0)
