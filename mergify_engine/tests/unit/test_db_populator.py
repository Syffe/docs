import pytest
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine.models import github_account
from mergify_engine.models import github_repository
from mergify_engine.tests.db_populator import DbPopulator


class DummyDataset(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        account = github_account.GitHubAccount(
            id=github_types.GitHubAccountIdType(
                cls.next_id(github_account.GitHubAccount)
            ),
            login=github_types.GitHubLogin("account_dds"),
            type="User",
        )

        session.add(
            github_repository.GitHubRepository(
                id=github_types.GitHubRepositoryIdType(
                    cls.next_id(github_repository.GitHubRepository)
                ),
                name=github_types.GitHubRepositoryName("dds_repo1"),
                owner=account,
                full_name="account_dds/dds_repo1",
                private=False,
                archived=False,
            )
        )

        session.add(
            github_repository.GitHubRepository(
                id=github_types.GitHubRepositoryIdType(
                    cls.next_id(github_repository.GitHubRepository)
                ),
                name=github_types.GitHubRepositoryName("dds_repo2"),
                owner=account,
                full_name="account_dds/dds_repo2",
                private=False,
                archived=False,
            )
        )


class AnotherDummyDataset(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        account = github_account.GitHubAccount(
            id=github_types.GitHubAccountIdType(
                cls.next_id(github_account.GitHubAccount)
            ),
            login=github_types.GitHubLogin("account_adds"),
            type="User",
        )

        session.add(
            github_repository.GitHubRepository(
                id=github_types.GitHubRepositoryIdType(
                    cls.next_id(github_repository.GitHubRepository)
                ),
                name=github_types.GitHubRepositoryName("adds_repo1"),
                owner=account,
                full_name="account_adds/adds_repo1",
                private=False,
                archived=False,
            )
        )

        session.add(
            github_repository.GitHubRepository(
                id=github_types.GitHubRepositoryIdType(
                    cls.next_id(github_repository.GitHubRepository)
                ),
                name=github_types.GitHubRepositoryName("adds_repo2"),
                owner=account,
                full_name="account_adds/adds_repo2",
                private=False,
                archived=False,
            )
        )


class DummyMetaDataset(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        await cls.load(session, {"DummyDataset", "AnotherDummyDataset"})


@pytest.mark.parametrize(
    "dataset, expected_accounts, expected_repos",
    [
        ({"DummyDataset"}, ["account_dds"], ["dds_repo1", "dds_repo2"]),
        ({"AnotherDummyDataset"}, ["account_adds"], ["adds_repo1", "adds_repo2"]),
        (
            {"DummyDataset", "AnotherDummyDataset"},
            ["account_adds", "account_dds"],
            ["adds_repo1", "adds_repo2", "dds_repo1", "dds_repo2"],
        ),
        (
            {"DummyMetaDataset"},
            ["account_adds", "account_dds"],
            ["adds_repo1", "adds_repo2", "dds_repo1", "dds_repo2"],
        ),
    ],
    ids=[
        "DummyDataset",
        "AnotherDummyDataset",
        "DummyDataset+AnotherDummyDataset",
        "DummyMetaDataset",
    ],
)
async def test_db_populator_dataset(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    dataset: set[str],
    expected_accounts: list[str],
    expected_repos: list[str],
) -> None:
    accounts = (await db.scalars(sqlalchemy.select(github_account.GitHubAccount))).all()
    assert accounts == []
    repos = (
        await db.scalars(sqlalchemy.select(github_repository.GitHubRepository))
    ).all()
    assert repos == []

    await DbPopulator.load(db, dataset)

    accounts = (
        await db.scalars(
            sqlalchemy.select(github_account.GitHubAccount).order_by(
                github_account.GitHubAccount.login
            )
        )
    ).all()
    assert len(accounts) == len(expected_accounts)
    for account, expected_account in zip(accounts, expected_accounts, strict=True):
        assert account.login == expected_account

    repos = (
        await db.scalars(
            sqlalchemy.select(github_repository.GitHubRepository).order_by(
                github_repository.GitHubRepository.name
            )
        )
    ).all()
    assert len(repos) == len(expected_repos)
    for repo, expected_repo in zip(repos, expected_repos, strict=True):
        assert repo.name == expected_repo


@pytest.mark.populated_db_datasets("AnotherDummyDataset")
async def test_populated_db_with_datasets(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    accounts = (
        await populated_db.scalars(sqlalchemy.select(github_account.GitHubAccount))
    ).all()
    assert len(accounts) == 1
    accounts[0].login = github_types.GitHubLogin("account_adds")

    repos = (
        await populated_db.scalars(
            sqlalchemy.select(github_repository.GitHubRepository).order_by(
                github_repository.GitHubRepository.name
            )
        )
    ).all()
    assert len(repos) == 2
    for repo, expected_repo in zip(repos, ["adds_repo1", "adds_repo2"], strict=True):
        assert repo.name == expected_repo


async def test_db_populator_dataset_registry_key_unicity() -> None:
    class FooBar(DbPopulator):
        pass

    class BarFoo(DbPopulator):
        pass

    # NOTE(Kontrolix) Create this one in a function to avoid ruff and mypy
    # screaming because a redefition of a class in the same scope
    def create_another_foo_bar() -> None:
        class FooBar(DbPopulator):
            pass

    with pytest.raises(
        ValueError, match="A dataset named 'FooBar' has already been registered."
    ):
        create_another_foo_bar()


async def test_db_populator_load_implementation(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    class BrokenDataset(DbPopulator):
        pass

    with pytest.raises(
        NotImplementedError, match="_load must be reimplemented on DbPopulator dataset"
    ):
        await DbPopulator.load(db, {"BrokenDataset"})
