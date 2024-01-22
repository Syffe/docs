import urllib.parse

import pytest
import respx

from mergify_engine import date
from mergify_engine import pagination
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
    pr1_full = pr1.copy() | {
        "maintainer_can_modify": True,
        "merged": False,
        "merged_by": None,
        "rebaseable": True,
        "mergeable": True,
        "mergeable_state": "clean",
        "changed_files": 1,
        "commits": 1,
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
    respx_mock.get(
        "https://api.github.com/repos/Mergifyio/engine/pulls/1",
    ).respond(200, json=pr1_full)

    resp = await web_client.request(
        "POST",
        "/v1/repos/Mergifyio/engine/pulls",
        params={"per_page": 30},
        json=["-closed"],
        headers={"Authorization": api_token.api_token},
    )

    assert resp.status_code == 200
    assert "pull_requests" in resp.json()
    assert len(resp.json()["pull_requests"]) == 1
    assert resp.json()["pull_requests"] == [pr1_full]

    resp = await web_client.request(
        "POST",
        "/v1/repos/Mergifyio/engine/pulls",
        params={"per_page": 30},
        json=["label=darkvader"],
        headers={"Authorization": api_token.api_token},
    )

    assert resp.status_code == 200
    assert "pull_requests" in resp.json()
    assert len(resp.json()["pull_requests"]) == 0

    resp = await web_client.request(
        "POST",
        "/v1/repos/Mergifyio/engine/pulls",
        params={"per_page": 30},
        json=["invalidcondition=darkvader"],
        headers={"Authorization": api_token.api_token},
    )

    assert resp.status_code == 400
    assert resp.json() == {
        "detail": "Invalid condition 'invalidcondition=darkvader'. Invalid attribute @ data[0]",
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

    pulls = [pr | {"number": i, "id": i, "labels": [kenobilabel]} for i in range(15)]

    pulls[7]["labels"] = [hellolabel]
    pulls[9]["labels"] = [hellolabel]

    # 4 pages because when we will hit the end of Github page 3, we will not have yet
    # 5 results to return, therefore we will have to call page 4 to see if there more
    # results to fetch.
    for i in range(4):
        respx_mock.get(
            "https://api.github.com/repos/Mergifyio/engine/pulls",
            params={
                "state": "open",
                "sort": "created",
                "direction": "desc",
                "per_page": 5,
                "page": i + 1,
            },
        ).respond(200, json=pulls[(i * 5) : 5 + (i * 5)])

    for pull in pulls:
        if pull["labels"] != [kenobilabel]:
            continue

        respx_mock.get(
            f"https://api.github.com/repos/Mergifyio/engine/pulls/{pull['number']}",
        ).respond(
            200,
            json=pull
            | {
                "maintainer_can_modify": True,
                "merged": False,
                "merged_by": None,
                "rebaseable": True,
                "mergeable": True,
                "mergeable_state": "clean",
                "changed_files": 1,
                "commits": 1,
            },
        )

    # First call is in sync with the github page 1
    resp = await web_client.post(
        "/v1/repos/Mergifyio/engine/pulls",
        params={"per_page": 5},
        json=["label=generalkenobi"],
        headers={"Authorization": api_token.api_token},
    )
    assert resp.status_code == 200

    resp_json = resp.json()
    assert len(resp_json["pull_requests"]) == 5
    assert resp_json["per_page"] == 5
    assert resp_json["size"] == 5
    assert [p["number"] for p in resp_json["pull_requests"]] == [0, 1, 2, 3, 4]

    next_link = resp.links["next"]["url"]
    next_cursor = pagination.Cursor.from_string(
        urllib.parse.parse_qs(urllib.parse.urlparse(next_link).query)["cursor"][0],
    )
    assert next_cursor.value(pagination.CursorType[tuple[int, int]]) == (2, 0)

    # Second call overlap github page 2 and 3
    resp = await web_client.post(
        next_link,
        json=["label=generalkenobi"],
        headers={"Authorization": api_token.api_token},
    )
    assert resp.status_code == 200

    resp_json = resp.json()
    assert len(resp_json["pull_requests"]) == 5
    assert resp_json["per_page"] == 5
    assert resp_json["size"] == 5
    assert [p["number"] for p in resp.json()["pull_requests"]] == [5, 6, 8, 10, 11]

    next_link = resp.links["next"]["url"]
    next_cursor = pagination.Cursor.from_string(
        urllib.parse.parse_qs(urllib.parse.urlparse(next_link).query)["cursor"][0],
    )
    assert next_cursor.value(pagination.CursorType[tuple[int, int]]) == (3, 2)

    # Thrid call restart on page 3 from where we stopped and return the last 3 PRs
    resp = await web_client.post(
        next_link,
        json=["label=generalkenobi"],
        headers={"Authorization": api_token.api_token},
    )
    assert resp.status_code == 200

    resp_json = resp.json()
    assert len(resp_json["pull_requests"]) == 3
    assert resp_json["per_page"] == 5
    assert resp_json["size"] == 3
    assert [p["number"] for p in resp.json()["pull_requests"]] == [12, 13, 14]

    #  We reached the end there should be no next link
    assert "next" not in resp.links


@pytest.mark.parametrize("cursor_value", ["blabla", ("abc", 123), (1, 2, 3), (1,)])
async def test_pulls_api_invalid_cursor(
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    cursor_value: object,
) -> None:
    invalid_cursor = pagination.Cursor(cursor_value, forward=True).to_string()
    resp = await web_client.request(
        "POST",
        "/v1/repos/Mergifyio/engine/pulls",
        params={"cursor": invalid_cursor},
        json=["base=main"],
        headers={"Authorization": api_token.api_token},
    )

    assert resp.status_code == 422
    assert resp.json() == {
        "message": "Invalid cursor",
        "cursor": invalid_cursor,
    }
