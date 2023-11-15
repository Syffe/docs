from unittest import mock

import anys
import pytest
import respx

from mergify_engine import context
from mergify_engine import subscription
from mergify_engine.tests import conftest as tests_conftest
from mergify_engine.tests.unit import conftest as tests_unit_conftest
from mergify_engine.tests.unit.api import conftest as tests_api_conftest


async def test_repository_configuration_simulator_success(
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
) -> None:
    mergify_yml = """
pull_request_rules:
  - name: automatic merge
    conditions:
      - status-success=test
      - "#approved-reviews-by>=2"
    actions:
      merge:
        method: squash
"""
    response = await web_client.post(
        "/v1/repos/Mergifyio/engine/configuration-simulator",
        headers={"Authorization": api_token.api_token},
        json={"mergify_yml": mergify_yml},
    )

    assert response.status_code == 200, response.json()
    assert response.json() == {"message": "The configuration is valid"}


async def test_repository_configuration_simulator_error(
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
) -> None:
    mergify_yml = """
pull_request_rules:
  - name: automatic merge
    conditions:
      - status-success=test
      - "#approved-reviews-by>=2"
    actions:
      something_wrong:
"""
    response = await web_client.post(
        "/v1/repos/Mergifyio/engine/configuration-simulator",
        headers={"Authorization": api_token.api_token},
        json={"mergify_yml": mergify_yml},
    )

    assert response.status_code == 422, response.json()
    assert response.json() == {
        "detail": [
            {
                "loc": ["body", "mergify_yml"],
                "msg": "extra keys not allowed @ pull_request_rules → item 0 → actions → something_wrong",
                "type": "mergify_config_error",
            },
        ],
    }


async def test_pull_request_configuration_simulator_success(
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    context_getter: tests_unit_conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
) -> None:
    mergify_yml = """
pull_request_rules:
  - name: automatic merge
    conditions:
      - base=main
    actions:
      merge:
        method: squash
queue_rules:
  - name: hotfix
    queue_conditions:
      - base=main
    merge_conditions:
      - label=merge
"""

    respx_mock.get(
        "https://api.github.com/repos/Mergifyio/mergify-engine/branches/main/protection",
    ).respond(200, json={})

    with mock.patch.object(
        context.Repository,
        "get_pull_request_context",
        context_getter,
    ):
        response = await web_client.post(
            "/v1/repos/Mergifyio/engine/pulls/123/configuration-simulator",
            headers={"Authorization": api_token.api_token},
            json={"mergify_yml": mergify_yml},
        )

    assert response.status_code == 200, response.json()
    assert response.json() == {
        "message": "The configuration is valid",
        "pull_request_rules": [
            {
                "name": "automatic merge",
                "conditions": anys.ANY_DICT,
                "actions": {"merge": anys.ANY_DICT},
            },
        ],
        "queue_rules": [
            {
                "name": "hotfix",
                "queue_conditions": anys.ANY_DICT,
                "merge_conditions": anys.ANY_DICT,
            },
        ],
    }

    # Assertions on pull request rules
    queue_conditions = response.json()["pull_request_rules"][0]["conditions"]
    assert queue_conditions == anys.AnyWithEntries(
        {"label": "all of", "match": True, "subconditions": anys.ANY_LIST},
    )
    conditions = queue_conditions["subconditions"]
    assert conditions == [
        anys.AnyWithEntries(
            {
                "label": "-conflict",
                "description": ":pushpin: merge requirement",
                "match": True,
            },
        ),
        anys.AnyWithEntries(
            {
                "label": "-draft",
                "description": ":pushpin: merge requirement",
                "match": True,
            },
        ),
        anys.AnyWithEntries(
            {
                "label": "-mergify-configuration-changed",
                "description": ":pushpin: merge -> allow_merging_configuration_change setting requirement",
                "match": True,
            },
        ),
        anys.AnyWithEntries(
            {
                "label": "base=main",
                "match": True,
            },
        ),
    ]
    actions = response.json()["pull_request_rules"][0]["actions"]
    assert actions["merge"] == {
        "allow_merging_configuration_change": False,
        "commit_message_template": None,
        "merge_bot_account": None,
        "method": "squash",
    }

    # Assertions on queue rules
    queue_conditions = response.json()["queue_rules"][0]["queue_conditions"]
    assert queue_conditions == anys.AnyWithEntries(
        {
            "label": "all of",
            "match": True,
            "subconditions": [
                anys.AnyWithEntries(
                    {
                        "label": "base=main",
                        "match": True,
                    },
                ),
            ],
        },
    )
    merge_conditions = response.json()["queue_rules"][0]["merge_conditions"]
    assert merge_conditions == anys.AnyWithEntries(
        {
            "label": "all of",
            "match": False,
            "subconditions": [
                anys.AnyWithEntries(
                    {
                        "label": "label=merge",
                        "match": False,
                    },
                ),
            ],
        },
    )


async def test_pull_request_configuration_simulator_success_with_template(
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    context_getter: tests_unit_conftest.ContextGetterFixture,
) -> None:
    mergify_yml = """
pull_request_rules:
  - name: automatic comment
    conditions:
      - base=main
    actions:
      comment:
        message: "Hello {{ author }}!"
"""

    with mock.patch.object(
        context.Repository,
        "get_pull_request_context",
        context_getter,
    ):
        response = await web_client.post(
            "/v1/repos/Mergifyio/engine/pulls/123/configuration-simulator",
            headers={"Authorization": api_token.api_token},
            json={"mergify_yml": mergify_yml},
        )

    assert response.status_code == 200, response.json()
    assert response.json() == {
        "message": "The configuration is valid",
        "pull_request_rules": [
            {
                "name": "automatic comment",
                "conditions": anys.ANY_DICT,
                "actions": {"comment": anys.ANY_DICT},
            },
        ],
        "queue_rules": anys.ANY_LIST,
    }
    actions = response.json()["pull_request_rules"][0]["actions"]
    assert actions["comment"] == {
        "message": "Hello contributor!",
        "bot_account": None,
    }


@pytest.mark.subscription(subscription.Features.CUSTOM_CHECKS)
async def test_pull_request_configuration_simulator_success_with_post_check(
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    context_getter: tests_unit_conftest.ContextGetterFixture,
) -> None:
    mergify_yml = """
pull_request_rules:
  - name: automatic check
    conditions:
      - base=main
    actions:
      post_check:
        summary: "Hello {{ author }}"
        success_conditions:
          - "base=main"
"""

    with mock.patch.object(
        context.Repository,
        "get_pull_request_context",
        context_getter,
    ):
        response = await web_client.post(
            "/v1/repos/Mergifyio/engine/pulls/123/configuration-simulator",
            headers={"Authorization": api_token.api_token},
            json={"mergify_yml": mergify_yml},
        )

    assert response.status_code == 200, response.json()
    assert response.json() == {
        "message": "The configuration is valid",
        "pull_request_rules": [
            {
                "name": "automatic check",
                "conditions": anys.ANY_DICT,
                "actions": {"post_check": anys.ANY_DICT},
            },
        ],
        "queue_rules": anys.ANY_LIST,
    }
    actions = response.json()["pull_request_rules"][0]["actions"]
    assert actions["post_check"] == {
        "always_show": False,
        "success_conditions": anys.ANY_DICT,
        "neutral_conditions": None,
        "summary": "Hello contributor",
        "title": "'automatic check' succeeded",
    }
    top_success_condition = actions["post_check"]["success_conditions"]
    assert top_success_condition == anys.AnyWithEntries(
        {"label": "all of", "match": True, "subconditions": anys.ANY_LIST},
    )
    success_conditions = top_success_condition["subconditions"]
    assert success_conditions == [
        anys.AnyWithEntries(
            {
                "label": "base=main",
                "match": True,
            },
        ),
    ]


async def test_pull_request_configuration_simulator_error_syntax(
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    context_getter: tests_unit_conftest.ContextGetterFixture,
) -> None:
    mergify_yml = """
pull_request_rules:
  - name: automatic merge
    conditions:
      - base=main
    actions:
      something_wrong:
"""

    with mock.patch.object(
        context.Repository,
        "get_pull_request_context",
        context_getter,
    ):
        response = await web_client.post(
            "/v1/repos/Mergifyio/engine/pulls/123/configuration-simulator",
            headers={"Authorization": api_token.api_token},
            json={"mergify_yml": mergify_yml},
        )

    assert response.status_code == 422, response.json()
    assert response.json() == {
        "detail": [
            {
                "loc": ["body", "mergify_yml"],
                "msg": "extra keys not allowed @ pull_request_rules → item 0 → actions → something_wrong",
                "type": "mergify_config_error",
            },
        ],
    }


async def test_pull_request_configuration_simulator_error_action(
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    context_getter: tests_unit_conftest.ContextGetterFixture,
) -> None:
    mergify_yml = """
pull_request_rules:
  - name: automatic merge
    conditions:
      - base=main
    actions:
      comment:
        message: null
"""

    with mock.patch.object(
        context.Repository,
        "get_pull_request_context",
        context_getter,
    ):
        response = await web_client.post(
            "/v1/repos/Mergifyio/engine/pulls/123/configuration-simulator",
            headers={"Authorization": api_token.api_token},
            json={"mergify_yml": mergify_yml},
        )

    assert response.status_code == 422, response.json()
    assert response.json() == {
        "detail": [
            {
                "details": "{'message': None, 'bot_account': None}",
                "loc": ["body", "mergify_yml"],
                "msg": "Cannot have `comment` action with no `message`",
                "type": "mergify_config_error",
            },
        ],
    }


async def test_pull_request_configuration_simulator_error_not_found(
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    context_getter: tests_unit_conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
) -> None:
    mergify_yml = """
pull_request_rules:
  - name: automatic merge
    conditions:
      - base=main
    actions:
      merge:
        method: squash
"""

    respx_mock.get("https://api.github.com/repos/Mergifyio/engine/pulls/404").respond(
        404,
        json={"message": "Not found"},
    )

    response = await web_client.post(
        "/v1/repos/Mergifyio/engine/pulls/404/configuration-simulator",
        headers={"Authorization": api_token.api_token},
        json={"mergify_yml": mergify_yml},
    )

    assert response.status_code == 404, response.json()
    assert response.json() == {"detail": "Not found"}


async def test_pull_request_configuration_simulator_invalid_body(
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    context_getter: tests_unit_conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
) -> None:
    with mock.patch.object(
        context.Repository,
        "get_pull_request_context",
        context_getter,
    ):
        response = await web_client.post(
            "/v1/repos/Mergifyio/engine/pulls/123/configuration-simulator",
            headers={"Authorization": api_token.api_token},
            json={"mergify_yml": "- no\n* way"},
        )

    assert response.status_code == 422, response.json()
    assert response.json() == {
        "detail": [
            {
                "loc": ["body", "mergify_yml"],
                "msg": """Invalid YAML @ line 2, column 2
```
while scanning an alias
  in "<unicode string>", line 2, column 1
did not find expected alphabetic or numeric character
  in "<unicode string>", line 2, column 2
```""",
                "type": "mergify_config_error",
            },
        ],
    }
