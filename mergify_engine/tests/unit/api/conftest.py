import typing

import httpx
import pytest
import respx
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine import subscription
from mergify_engine.models.github import user as github_user
from mergify_engine.tests import conftest


class TokenUserRepo(typing.NamedTuple):
    api_token: str
    user: github_user.GitHubUser
    repo: github_types.GitHubRepository


@pytest.fixture
async def api_token(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    respx_mock: respx.MockRouter,
) -> TokenUserRepo:
    user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(42),
        login=github_types.GitHubLogin("Mergifyio"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    # Mock different GitHub responses
    gh_owner = github_types.GitHubAccount(
        id=github_types.GitHubAccountIdType(0),
        login=github_types.GitHubLogin("Mergifyio"),
        type="User",
        avatar_url="",
    )

    repo = github_types.GitHubRepository(
        {
            "id": github_types.GitHubRepositoryIdType(0),
            "private": False,
            "archived": False,
            "name": github_types.GitHubRepositoryName("engine"),
            "full_name": "Mergifyio/engine",
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType("main"),
            "owner": gh_owner,
        }
    )

    # get installation from repository
    respx_mock.get(
        "https://api.github.com/repos/Mergifyio/engine/installation"
    ).respond(200, json={"account": gh_owner, "id": 42, "suspended_at": None})

    # get the repository
    respx_mock.get("https://api.github.com/repos/Mergifyio/engine").respond(
        200,
        json=repo,  # type: ignore[arg-type]
    )

    # get membership for the auth user
    respx_mock.get("https://api.github.com/user/memberships/orgs/0").respond(
        status_code=200,
        json={
            "state": "active",
            "role": "admin",
            "user": {"id": user.id, "login": user.login},
            "organization": {"id": 42, "login": "Mergifyio"},
        },
    )

    # get a github access token
    respx_mock.post(
        "https://api.github.com/app/installations/42/access_tokens"
    ).respond(
        200,
        json=github_types.GitHubInstallationAccessToken(
            {
                "token": "gh_token",
                "expires_at": "2111-09-08T17:26:27Z",
            }
        ),  # type: ignore[arg-type]
    )
    # NOTE(sileht): We don't care if access token is used ot not,
    # so call it once to please respx.assert_all_called()
    httpx.post(
        "https://api.github.com/app/installations/42/access_tokens", json={"foo": "bar"}
    )

    # get account subscription to Mergify
    respx_mock.get(
        f"http://localhost:5000/engine/subscription/{gh_owner['id']}"
    ).respond(
        200,
        json={
            "subscription_active": True,
            "subscription_reason": "",
            "features": [f.value for f in subscription.Features],
        },
    )

    # 2) Create app and get an access token
    await web_client.log_as(user.id)
    logged_as = await web_client.logged_as()
    assert logged_as == "Mergifyio"

    # create application key
    resp = await web_client.post(
        f"/front/github-account/{gh_owner['id']}/applications",
        json={"name": "Mergifyio"},
    )

    data = resp.json()
    return TokenUserRepo(
        f"bearer {data['api_access_key']}{data['api_secret_key']}", user, repo
    )
