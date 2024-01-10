import re
import typing
from unittest import mock

import pytest

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine.engine import actions_runner
from mergify_engine.rules import conditions
from mergify_engine.rules.config import conditions as cond_config
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.tests.unit import conftest
from mergify_engine.yaml import yaml


@mock.patch.object(
    conditions,
    "get_queue_conditions",
    mock.AsyncMock(return_value=None),
)
@pytest.mark.parametrize(
    ("merged_by", "merged_by_id", "raw_config", "result"),
    [
        (
            None,
            None,
            """
pull_request_rules:
 - name: Automatic merge on approval
   conditions:
     - and:
       - "-draft"
       - "author=contributor"
   actions:
     merge:
       method: merge
""",
            "",
        ),
        (
            "foobar",
            github_types.GitHubAccountIdType(1),
            """
queue_rules:
  - name: foo
    conditions: []
pull_request_rules:
 - name: Automatic queue on approval
   conditions:
     - and:
       - "-draft"
       - "author=contributor"
   actions:
     queue:
       name: foo
        """,
            "⚠️ The pull request has been merged by @foobar\n\n",
        ),
        (
            None,
            None,
            """
pull_request_rules:
 - name: Automatic queue on approval
   conditions:
     - and:
       - "-draft"
       - "author=contributor"
   actions:
     delete_head_branch:
        """,
            "⚠️ The pull request has been closed by GitHub because its commits are also part of another pull request\n\n",
        ),
    ],
)
async def test_get_already_merged_summary(
    merged_by: github_types.GitHubLogin | None,
    merged_by_id: github_types.GitHubAccountIdType | None,
    raw_config: str,
    result: str,
    context_getter: conftest.ContextGetterFixture,
    fake_mergify_bot: github_types.GitHubAccount,
) -> None:
    if merged_by is None:
        merged_by = fake_mergify_bot["login"]
    if merged_by_id is None:
        merged_by_id = fake_mergify_bot["id"]

    ctxt = await context_getter(
        github_types.GitHubPullRequestNumber(1),
        merged=True,
        merged_by=github_types.GitHubAccount(
            {
                "id": merged_by_id,
                "login": merged_by,
                "type": "User",
                "avatar_url": "",
            },
        ),
    )
    ctxt.repository._caches.branch_protections[
        github_types.GitHubRefType("main")
    ] = None

    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content=raw_config,
        encoding="base64",
    )

    config = await mergify_conf.get_mergify_config_from_file(ctxt.repository, file)
    ctxt.repository._caches.mergify_config.set(config)
    match = await config["pull_request_rules"].get_pull_request_rules_evaluator(ctxt)
    assert result == await actions_runner.get_already_merged_summary(ctxt, match)


async def test_get_already_merged_summary_without_rules(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    merged_by = github_types.GitHubLogin("foobar")
    merged_by_id = github_types.GitHubAccountIdType(1)

    ctxt = await context_getter(
        github_types.GitHubPullRequestNumber(1),
        merged=True,
        merged_by=github_types.GitHubAccount(
            {
                "id": merged_by_id,
                "login": merged_by,
                "type": "User",
                "avatar_url": "",
            },
        ),
    )
    ctxt.repository._caches.branch_protections[
        github_types.GitHubRefType("main")
    ] = None

    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content="",
        encoding="base64",
    )

    config = await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)
    match = await config["pull_request_rules"].get_pull_request_rules_evaluator(ctxt)
    assert not (await actions_runner.get_already_merged_summary(ctxt, match))


@pytest.mark.parametrize(
    ("config_key", "config_value", "expected_value"),
    [
        (
            "bot_account",
            "sileht",
            "{bot_account: sileht}\n",
        ),
        (
            "bot_account",
            {"login": "sileht", "oauth_token": "xxxxx"},
            "{bot_account: sileht}\n",
        ),
        ("set", {"foo", "bar"}, ("set: [foo, bar]\n", "set: [bar, foo]\n")),
        (
            "re",
            re.compile("foobar"),
            "{re: foobar}\n",
        ),
        ("str", "str", "{str: str}\n"),
        (
            "multiline1",
            "str\nstr\nstr",
            "multiline1: |-\n  str\n  str\n  str\n",
        ),
        (
            "multiline2",
            "str\nstr\nstr\n",
            "multiline2: |\n  str\n  str\n  str\n",
        ),
        (
            "multiline3",
            "\nstr\nstr\nstr\n",
            "multiline3: |2\n\n  str\n  str\n  str\n",
        ),
        (
            "conditions",
            conditions.PullRequestRuleConditions(
                [
                    cond_config.RuleConditionSchema(
                        {"not": {"or": ["base=main", "label=foo"]}},
                    ),
                ],
            ),
            "conditions: |-\n  - [ ] not:\n    - [ ] any of:\n      - [ ] `base=main`\n      - [ ] `label=foo`\n",
        ),
    ],
)
async def test_sanitize_action_config(
    config_key: str,
    config_value: typing.Any,
    expected_value: typing.Any,
) -> None:
    final_value = yaml.safe_dump(
        {config_key: actions_runner._sanitize_action_config(config_key, config_value)},
    )

    if isinstance(expected_value, tuple):
        assert final_value in expected_value
    else:
        assert final_value == expected_value
