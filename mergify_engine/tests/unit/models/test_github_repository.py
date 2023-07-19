import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine.models import github_repository


async def test_get_by_name(db: sqlalchemy.ext.asyncio.AsyncSession) -> None:
    # Insert a first repository
    account1 = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(1),
            "login": github_types.GitHubLogin("account1"),
            "type": "User",
            "avatar_url": "",
        }
    )
    account1_repo1 = github_types.GitHubRepository(
        {
            "id": github_types.GitHubRepositoryIdType(1),
            "name": github_types.GitHubRepositoryName("repo1"),
            "owner": account1,
            "full_name": "account1/repo1",
            "private": False,
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType("main"),
        }
    )
    expected_repo = await github_repository.GitHubRepository.get_or_create(
        db, account1_repo1
    )
    db.add(expected_repo)

    # Insert a second repository with the same name, but different owner
    account2 = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(2),
            "login": github_types.GitHubLogin("account2"),
            "type": "User",
            "avatar_url": "",
        }
    )
    account2_repo1 = github_types.GitHubRepository(
        {
            "id": github_types.GitHubRepositoryIdType(2),
            "name": github_types.GitHubRepositoryName("repo1"),
            "owner": account2,
            "full_name": "account2/repo1",
            "private": False,
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType("main"),
        }
    )
    another_repo = await github_repository.GitHubRepository.get_or_create(
        db, account2_repo1
    )
    db.add(another_repo)
    await db.commit()

    actual_repo = await github_repository.GitHubRepository.get_by_name(
        db,
        github_types.GitHubAccountIdType(1),
        github_types.GitHubRepositoryName("repo1"),
    )

    assert actual_repo == expected_repo


async def test_as_dict(db: sqlalchemy.ext.asyncio.AsyncSession) -> None:
    gh_owner = github_types.GitHubAccount(
        {
            "login": github_types.GitHubLogin("Mergifyio"),
            "id": github_types.GitHubAccountIdType(0),
            "type": "User",
            "avatar_url": "",
        }
    )
    gh_repo = github_types.GitHubRepository(
        {
            "full_name": "Mergifyio/mergify-engine",
            "name": github_types.GitHubRepositoryName("mergify-engine"),
            "private": False,
            "id": github_types.GitHubRepositoryIdType(0),
            "owner": gh_owner,
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType("main"),
        }
    )
    repo = await github_repository.GitHubRepository.get_or_create(db, gh_repo)
    db.add(repo)
    await db.commit()

    commited_repo = await github_repository.GitHubRepository.get_by_name(
        db,
        github_types.GitHubAccountIdType(0),
        github_types.GitHubRepositoryName("mergify-engine"),
    )

    assert commited_repo is not None
    assert commited_repo.as_dict() == {
        "id": 0,
        "name": "mergify-engine",
        "owner_id": 0,
        "owner": {"id": 0, "login": "Mergifyio", "type": "User"},
        "private": False,
        "default_branch": "main",
        "full_name": "Mergifyio/mergify-engine",
        "archived": False,
    }


def test_is_complete() -> None:
    repo = github_repository.GitHubRepository()
    assert not repo.is_complete()

    repo = github_repository.GitHubRepository(
        id=github_types.GitHubRepositoryIdType(0),
        owner_id=github_types.GitHubAccountIdType(0),
        name=github_types.GitHubRepositoryName("hello"),
    )
    assert not repo.is_complete()

    repo = github_repository.GitHubRepository(
        id=github_types.GitHubRepositoryIdType(0),
        owner_id=github_types.GitHubAccountIdType(0),
        name=github_types.GitHubRepositoryName("hello"),
        private=False,
        default_branch=github_types.GitHubRefType("main"),
        full_name="hello/there",
        archived=False,
    )
    assert repo.is_complete()
