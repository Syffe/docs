from urllib import parse

import respx

from mergify_engine import date
from mergify_engine.tests import conftest as tests_conftest
from mergify_engine.tests.unit.api import conftest as tests_api_conftest


async def test_pulls_api_filters(
    respx_mock: respx.MockRouter,
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
) -> None:
    pr1 = {
        "number": 1,
        "head": {
            "ref": "test123",
            "user": api_token.user.to_github_account(),
            "repo": api_token.repo,
            "sha": "12345",
            "label": "test123",
        },
        "base": {
            "ref": "main",
            "user": api_token.user.to_github_account(),
            "repo": api_token.repo,
            "sha": "12344",
            "label": "main",
        },
        "labels": [
            {
                "id": 0,
                "name": "hellothere",
                "color": "#FFFFFF",
                "default": False,
            },
            {
                "id": 1,
                "name": "generalkenobi",
                "color": "#FFFFFE",
                "default": False,
            },
        ],
        "user": api_token.user.to_github_account(),
        "locked": False,
        "merge_commit_sha": None,
        "state": "open",
        "draft": False,
        "id": 1,
        "merged_at": None,
        "html_url": "http://whatever.com",
        "issue_url": "http://whatever.com",
        "title": "TITLE",
        "body": "BODY",
        "assignees": [],
        "requested_reviewers": [],
        "requested_teams": [],
        "milestone": None,
        "updated_at": date.utcnow().isoformat(),
        "created_at": date.utcnow().isoformat(),
        "closed_at": None,
        "node_id": "",
    }
    respx_mock.get(
        "https://api.github.com/repos/Mergifyio/engine/pulls",
        params={
            "state": "open",
            "sort": "created",
            "direction": "desc",
            "per_page": 30,
            "page": 1,
        },
    ).respond(200, json=[pr1])

    resp = await web_client.request(
        "POST",
        "/v1/repos/Mergifyio/engine/pulls",
        json=[],
        headers={"Authorization": api_token.api_token},
    )

    assert resp.status_code == 200
    assert "pull_requests" in resp.json()
    assert len(resp.json()["pull_requests"]) == 1
    assert resp.json()["pull_requests"] == [pr1]

    resp = await web_client.request(
        "POST",
        "/v1/repos/Mergifyio/engine/pulls",
        json=["label=darkvader"],
        headers={"Authorization": api_token.api_token},
    )

    assert resp.status_code == 200
    assert "pull_requests" in resp.json()
    assert len(resp.json()["pull_requests"]) == 0

    resp = await web_client.request(
        "POST",
        "/v1/repos/Mergifyio/engine/pulls",
        json=["invalidcondition=darkvader"],
        headers={"Authorization": api_token.api_token},
    )

    assert resp.status_code == 400
    assert resp.json() == {
        "detail": "Invalid condition 'invalidcondition=darkvader'. Invalid attribute"
    }


async def test_pulls_api_pagination(
    respx_mock: respx.MockRouter,
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
) -> None:
    pr = {
        "number": 1,
        "head": {
            "ref": "test123",
            "user": api_token.user.to_github_account(),
            "repo": api_token.repo,
            "sha": "12345",
            "label": "test123",
        },
        "base": {
            "ref": "main",
            "user": api_token.user.to_github_account(),
            "repo": api_token.repo,
            "sha": "12344",
            "label": "main",
        },
        "labels": [],
        "user": api_token.user.to_github_account(),
        "locked": False,
        "merge_commit_sha": None,
        "state": "open",
        "merged": False,
        "draft": False,
        "id": 1,
        "merged_at": None,
        "rebaseable": True,
        "html_url": "http://whatever.com",
        "issue_url": "http://whatever.com",
        "title": "TITLE",
        "body": "BODY",
        "assignees": [],
        "requested_reviewers": [],
        "requested_teams": [],
        "milestone": None,
        "updated_at": date.utcnow().isoformat(),
        "created_at": date.utcnow().isoformat(),
        "closed_at": None,
        "node_id": "",
    }

    hellolabel = {
        "id": 1,
        "name": "hellothere",
        "color": "#FFFFFF",
        "default": False,
    }
    kenobilabel = {
        "id": 2,
        "name": "generalkenobi",
        "color": "#FFFFFE",
        "default": False,
    }

    pulls_p1 = [
        pr
        | {
            "number": i,
            "id": i,
            "labels": [hellolabel],
        }
        for i in range(30)
    ]
    pulls_p1[-1]["labels"] = [kenobilabel]
    pulls_p1[-2]["labels"] = [kenobilabel]

    pulls_p2 = [
        pr
        | {
            "number": i,
            "id": i,
            "labels": [kenobilabel],
        }
        for i in range(30, 60)
    ]

    respx_mock.get(
        "https://api.github.com/repos/Mergifyio/engine/pulls",
        params={
            "state": "open",
            "sort": "created",
            "direction": "desc",
            "per_page": 30,
            "page": 1,
        },
    ).respond(200, json=pulls_p1)

    respx_mock.get(
        "https://api.github.com/repos/Mergifyio/engine/pulls",
        params={
            "state": "open",
            "sort": "created",
            "direction": "desc",
            "per_page": 30,
            "page": 2,
        },
    ).respond(200, json=pulls_p2)

    resp = await web_client.request(
        "POST",
        "/v1/repos/Mergifyio/engine/pulls",
        json=["label=generalkenobi"],
        headers={"Authorization": api_token.api_token},
    )

    assert resp.status_code == 200
    assert "pull_requests" in resp.json()
    assert len(resp.json()["pull_requests"]) == 10
    assert [p["number"] for p in resp.json()["pull_requests"]] == list(range(28, 38))

    links = resp.headers["link"].split(",")
    next_link = None
    for link in links:
        if link.endswith('rel="next"'):
            next_link = link
            break

    assert next_link is not None, "Should have a 'next' link in headers"

    next_url_str = next_link.removesuffix('>; rel="next"')
    next_url_str = next_url_str.removeprefix("<")

    next_url_parsed = parse.urlparse(next_url_str)

    resp = await web_client.request(
        "POST",
        f"{next_url_parsed.path}?{next_url_parsed.query}",
        json=["label=generalkenobi"],
        headers={"Authorization": api_token.api_token},
    )

    assert resp.status_code == 200
    assert "pull_requests" in resp.json()
    assert len(resp.json()["pull_requests"]) == 10
    assert [p["number"] for p in resp.json()["pull_requests"]] == list(range(38, 48))


async def test_pulls_api_invalid_cursor(
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
) -> None:
    resp = await web_client.request(
        "POST",
        "/v1/repos/Mergifyio/engine/pulls",
        params={"cursor": "blabla"},
        json=["base=main"],
        headers={"Authorization": api_token.api_token},
    )

    assert resp.status_code == 400
    assert resp.json() == {"detail": "Invalid page cursor"}

    resp = await web_client.request(
        "POST",
        "/v1/repos/Mergifyio/engine/pulls",
        params={"cursor": "abc-123"},
        json=["base=main"],
        headers={"Authorization": api_token.api_token},
    )

    assert resp.status_code == 400
    assert resp.json() == {"detail": "Invalid page cursor"}
