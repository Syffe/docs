import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine.models import github_repository


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
        db, github_types.GitHubRepositoryName("mergify-engine")
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
