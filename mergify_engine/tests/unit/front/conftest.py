import pytest
import respx

from mergify_engine import github_types


@pytest.fixture
def front_login_mock(respx_mock: respx.MockRouter) -> None:
    github_api_user = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(42),
            "login": github_types.GitHubLogin("user-login"),
            "avatar_url": "http://example.com/logo",
            "type": "User",
        }
    )

    respx_mock.post(
        "http://localhost:5000/engine/user-update",
        json={"user": github_api_user, "token": "user-token"},
    ).respond(204)
    respx_mock.get("/user").respond(200, json=dict(github_api_user))
    respx_mock.get("/user/installations/123/repositories").respond(
        200, json=[{"owner": github_api_user}]
    )
    respx_mock.get("/user/installations/456/repositories").respond(404)
