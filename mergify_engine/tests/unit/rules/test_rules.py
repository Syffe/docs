from base64 import encodebytes
from collections import abc
import copy
import datetime
import re
import typing
from unittest import mock

import pytest
import respx
import sqlalchemy.ext.asyncio
import voluptuous

from mergify_engine import context
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import redis_utils
from mergify_engine import rules
from mergify_engine import settings
from mergify_engine.clients import http
from mergify_engine.models import github_repository
from mergify_engine.rules import conditions
from mergify_engine.rules.config import conditions as cond_config
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.tests import utils
from mergify_engine.tests.unit import conftest


def pull_request_rule_from_list(lst: typing.Any) -> prr_config.PullRequestRules:
    return typing.cast(
        prr_config.PullRequestRules,
        voluptuous.Schema(prr_config.get_pull_request_rules_schema())(lst),
    )


def test_valid_condition() -> None:
    c = conditions.RuleCondition.from_string("head~=bar")
    assert str(c) == "head~=bar"


def fake_expander(v: str) -> list[str]:
    return ["foo", "bar"]


async def test_expanders() -> None:
    rc = conditions.RuleCondition.from_string("author=@team")
    rc.filters.boolean.value_expanders["author"] = fake_expander
    await rc(mock.Mock(author="foo"))
    assert rc.match

    copy_rc = rc.copy()
    await copy_rc(mock.Mock(author="foo"))
    assert copy_rc.match


def test_invalid_condition_re() -> None:
    with pytest.raises(voluptuous.Invalid):
        conditions.RuleCondition.from_string("head~=(bar")


async def test_multiple_pulls_to_match() -> None:
    c = conditions.QueueRuleMergeConditions(
        [
            conditions.RuleConditionCombination(
                {
                    "or": [
                        conditions.RuleCondition.from_string("base=main"),
                        conditions.RuleCondition.from_string("base=main"),
                    ]
                }
            )
        ]
    )
    assert await c([conftest.FakePullRequest({"number": 1, "base": "main"})])
    c = c.copy()
    assert await c([conftest.FakePullRequest({"number": 1, "base": "main"})])
    c = c.copy()
    assert not await c([conftest.FakePullRequest({"number": 1, "base": "other"})])
    c = c.copy()
    assert await c(
        [
            conftest.FakePullRequest({"number": 1, "base": "main"}),
            conftest.FakePullRequest({"number": 1, "base": "main"}),
        ]
    )
    c = c.copy()
    assert await c(
        [
            conftest.FakePullRequest({"number": 1, "base": "main"}),
            conftest.FakePullRequest({"number": 1, "base": "main"}),
            conftest.FakePullRequest({"number": 1, "base": "main"}),
        ]
    )
    c = c.copy()
    assert not await c(
        [
            conftest.FakePullRequest({"number": 1, "base": "main"}),
            conftest.FakePullRequest({"number": 1, "base": "main"}),
            conftest.FakePullRequest({"number": 1, "base": "other"}),
        ]
    )


@pytest.mark.parametrize(
    "valid",
    (
        {"name": "hello", "conditions": ["head:main"], "actions": {}},
        {"name": "hello", "conditions": ["body:foo", "body:baz"], "actions": {}},
        {
            "name": "and",
            "conditions": [{"and": ["body:foo", "body:baz"]}],
            "actions": {},
        },
        {
            "name": "and 1",
            "conditions": [{"and": ["body:foo"]}],
            "actions": {},
        },
        {"name": "or", "conditions": [{"or": ["body:foo", "body:baz"]}], "actions": {}},
        {"name": "or 1", "conditions": [{"or": ["body:foo"]}], "actions": {}},
        {
            "name": "and,or",
            "conditions": [{"and": ["label=foo", {"or": ["body:foo", "body:baz"]}]}],
            "actions": {},
        },
        {
            "name": "or,and",
            "conditions": [{"or": ["label=foo", {"and": ["body:foo", "body:baz"]}]}],
            "actions": {},
        },
    ),
)
def test_pull_request_rule(valid: typing.Any) -> None:
    pull_request_rule_from_list([valid])


@pytest.mark.parametrize(
    "invalid,error",
    (
        (
            {
                "name": "unknown operator",
                "conditions": [{"what": ["base:foo", "base:baz"]}],
                "actions": {},
            },
            "extra keys not allowed @ data[0]['conditions'][0]['what']",
        ),
        (
            {
                "name": "too many nested conditions",
                "conditions": [
                    {
                        "or": [
                            "label=foo",
                            {
                                "and": [
                                    {
                                        "or": [
                                            "base:foo",
                                            {
                                                "and": [
                                                    "base:baz",
                                                    "base=main",
                                                    {
                                                        "or": [
                                                            "author=bot",
                                                            {
                                                                "and": [
                                                                    "author:robot",
                                                                    "label=robot",
                                                                    {
                                                                        "or": [
                                                                            "#approved-reviews-by>=1",
                                                                            {
                                                                                "and": [
                                                                                    "#review-threads-unresolved=0",
                                                                                    {
                                                                                        "or": [
                                                                                            "#approved-reviews-by=0",
                                                                                            "label=skip",
                                                                                        ],
                                                                                    },
                                                                                ],
                                                                            },
                                                                        ],
                                                                    },
                                                                ]
                                                            },
                                                        ],
                                                    },
                                                ]
                                            },
                                        ]
                                    },
                                    "label=bar",
                                ]
                            },
                        ]
                    }
                ],
                "actions": {},
            },
            "Maximun number of nested conditions reached",
        ),
    ),
)
def test_invalid_pull_request_rule(invalid: typing.Any, error: str) -> None:
    with pytest.raises(voluptuous.Invalid) as i:
        pull_request_rule_from_list([invalid])

    assert error in str(i.value)


async def test_same_pull_request_rules_name() -> None:
    with pytest.raises(mergify_conf.InvalidRules) as x:
        await mergify_conf.get_mergify_config_from_dict(
            mock.MagicMock(),
            {
                "pull_request_rules": [
                    {
                        "name": "merge on main",
                        "conditions": ["base=new_rule", "-merged"],
                        "actions": {"merge": {}},
                    },
                    {
                        "name": "merge on main",
                        "conditions": ["base=new_rule", "-merged"],
                        "actions": {"merge": {}},
                    },
                ]
            },
            "",
        )
    assert (
        str(x.value)
        == "pull_request_rules names must be unique, found `merge on main` twice for dictionary value @ pull_request_rules"
    )


async def test_same_queue_rules_name() -> None:
    with pytest.raises(mergify_conf.InvalidRules) as x:
        await mergify_conf.get_mergify_config_from_dict(
            mock.MagicMock(),
            {
                "queue_rules": [
                    {
                        "name": "default",
                        "merge_conditions": ["schedule: MON-FRI 08:00-17:00"],
                        "allow_inplace_checks": False,
                    },
                    {
                        "name": "default",
                        "merge_conditions": ["schedule: MON-FRI 08:00-17:00"],
                        "allow_inplace_checks": False,
                    },
                ]
            },
            "",
        )
    assert (
        str(x.value)
        == "queue_rules names must be unique, found `default` twice for dictionary value @ queue_rules"
    )


async def test_fallback_partition_attribute_without_empty_conditions() -> None:
    with pytest.raises(mergify_conf.InvalidRules) as x:
        await mergify_conf.get_mergify_config_from_dict(
            mock.MagicMock(),
            {
                "partition_rules": [
                    {
                        "name": "projectA",
                        "conditions": ["schedule: MON-FRI 08:00-17:00"],
                        "fallback_partition": True,
                    },
                ]
            },
            "",
        )
    assert (
        str(x.value)
        == "conditions of partition `projectA` must be empty to use `fallback_partition` attribute for dictionary value @ partition_rules"
    )


async def test_double_fallback_partition_attribute() -> None:
    with pytest.raises(mergify_conf.InvalidRules) as x:
        await mergify_conf.get_mergify_config_from_dict(
            mock.MagicMock(),
            {
                "partition_rules": [
                    {
                        "name": "projectA",
                        "fallback_partition": True,
                    },
                    {
                        "name": "projectB",
                        "fallback_partition": True,
                    },
                ]
            },
            "",
        )
    assert (
        str(x.value)
        == "found more than one usage of `fallback_partition` attribute, it must be used only once for dictionary value @ partition_rules"
    )


async def test_same_partition_rules_name() -> None:
    with pytest.raises(mergify_conf.InvalidRules) as x:
        await mergify_conf.get_mergify_config_from_dict(
            mock.MagicMock(),
            {
                "partition_rules": [
                    {
                        "name": "projectA",
                        "conditions": ["schedule: MON-FRI 08:00-17:00"],
                    },
                    {
                        "name": "projectA",
                        "conditions": ["schedule: MON-FRI 08:00-17:00"],
                    },
                ]
            },
            "",
        )
    assert (
        str(x.value)
        == "partition_rules names must be unique, found `projectA` twice for dictionary value @ partition_rules"
    )


async def test_default_partition_name_used() -> None:
    with pytest.raises(mergify_conf.InvalidRules) as x:
        await mergify_conf.get_mergify_config_from_dict(
            mock.MagicMock(),
            {
                "partition_rules": [
                    {
                        "name": partr_config.DEFAULT_PARTITION_NAME,
                        "conditions": ["schedule: MON-FRI 08:00-17:00"],
                    },
                    {
                        "name": "projectA",
                        "conditions": ["schedule: MON-FRI 08:00-17:00"],
                    },
                ]
            },
            "",
        )
    assert (
        str(x.value)
        == f"`{partr_config.DEFAULT_PARTITION_NAME}` is a reserved partition name and cannot be used for dictionary value @ partition_rules"
    )


async def test_jinja_with_list_attribute() -> None:
    config = await utils.load_mergify_config(
        """
            pull_request_rules:
              - name: ahah
                conditions:
                - base=main
                actions:
                  comment:
                    message: |
                      This pull request has been approved by:
                      {% for name in label %}
                      @{{name}}
                      {% endfor %}
                      {% for name in files %}
                      @{{name}}
                      {% endfor %}
                      {% for name in assignee %}
                      @{{name}}
                      {% endfor %}
                      {% for name in approved_reviews_by %}
                      @{{name}}
                      {% endfor %}
                      Thank you @{{author}} for your contributions!
            """
    )
    assert [rule.name for rule in config["pull_request_rules"]] == [
        "ahah",
    ]


def test_jinja_filters() -> None:
    rules.UserConfigurationSchema(
        rules.YamlSchema(
            """queue_rules:
  - name: default
    conditions: []

pull_request_rules:
  - name: ahah
    conditions:
    - base=main
    actions:
      queue:
        name: default
        commit_message_template: |
          Merge PR #{{ number }} - {{ title }}
          {{ body | get_section("## Commit Message", "") }}
"""
        )
    )
    rules.UserConfigurationSchema(
        rules.YamlSchema(
            """queue_rules:
  - name: default
    conditions: []

pull_request_rules:
  - name: ahah
    conditions:
    - base=main
    actions:
      queue:
        name: default
        commit_message_template: |
          Merge PR #{{ number }} - {{ title }}
          {{ body | get_section("## Commit Message") | markdownify }}
"""
        )
    )


def test_jinja_with_wrong_syntax() -> None:
    with pytest.raises(voluptuous.Invalid) as i:
        rules.UserConfigurationSchema(
            rules.YamlSchema(
                """pull_request_rules:
  - name: ahah
    conditions:
    - base=main
    actions:
      comment:
        message: |
          This pull request has been approved by:
          {% for name in approved_reviews_by %}
          Thank you @{{author}} for your contributions!
"""
            )
        )
    assert str(i.value) == (
        "Template syntax error @ data['pull_request_rules']"
        "[0]['actions']['comment']['message'][line 3]"
    )

    with pytest.raises(voluptuous.Invalid) as i:
        rules.UserConfigurationSchema(
            rules.YamlSchema(
                """pull_request_rules:
  - name: ahah
    conditions:
    - base=main
    actions:
      comment:
        message: |
          This pull request has been approved by:
          {% for name in approved_reviews_by %}
          @{{ name }}
          {% endfor %}
          Thank you @{{foo}} for your contributions!
"""
            )
        )
    assert str(i.value) == (
        "Template syntax error for dictionary value @ data['pull_request_rules']"
        "[0]['actions']['comment']['message']"
    )


@pytest.mark.parametrize(
    "valid",
    (
        (
            """
            pull_request_rules:
              - name: ahah
                conditions:
                - base=main
                actions:
                  comment:
                    message: |
                      This pull request has been approved by
            """
        ),
        (
            """
            pull_request_rules:
              - name: ahah
                conditions:
                - base=main
                actions:
                  comment:
                    message: |
                      This pull request has been approved by
                      {% for name in approved_reviews_by %}
                      @{{ name }}
                      {% endfor %}
            """
        ),
        (
            """
            defaults:
              actions:
                rebase:
                  bot_account: test-bot-account
            pull_request_rules:
              - name: ahah
                conditions:
                - base=main
                actions:
                  comment:
                    message: |
                      This pull request has been approved by
            """
        ),
        (
            """
            defaults:
              actions:
                comment:
                  message: I love Mergify
                rebase:
                  bot_account: test-bot-account
            pull_request_rules:
              - name: ahah
                conditions:
                - base=main
                actions:
                  comment:
                    message: Hey
                  rebase: {}
            """
        ),
        (
            """
            commands_restrictions:
              backport:
                conditions:
                - base=main
            """
        ),
    ),
)
async def test_get_mergify_config(
    valid: str, fake_repository: context.Repository
) -> None:
    async def item(
        *args: typing.Any, **kwargs: typing.Any
    ) -> github_types.GitHubContentFile:
        return github_types.GitHubContentFile(
            {
                "content": encodebytes(valid.encode()).decode(),
                "path": github_types.GitHubFilePath(".mergify.yml"),
                "type": "file",
                "sha": github_types.SHAType("azertyu"),
            }
        )

    client = mock.Mock()
    client.item.return_value = item()
    fake_repository.installation.client = client

    config_file = await fake_repository.get_mergify_config_file()
    assert config_file is not None
    schema = await mergify_conf.get_mergify_config_from_file(
        mock.MagicMock(), config_file
    )
    assert isinstance(schema, dict)
    assert "pull_request_rules" in schema
    assert "queue_rules" in schema
    assert "defaults" in schema
    assert "commands_restrictions" in schema
    assert "partition_rules" in schema


async def test_get_mergify_config_with_defaults(
    fake_repository: context.Repository,
) -> None:
    config = """
defaults:
  actions:
    comment:
      message: "Hey"
      bot_account: foo-bot
    rebase:
      bot_account: test-bot-account
pull_request_rules:
  - name: ahah
    conditions:
    - base=main
    actions:
      comment:
        message: I love Mergify
      rebase: {}
"""

    async def item(
        *args: typing.Any, **kwargs: typing.Any
    ) -> github_types.GitHubContentFile:
        return github_types.GitHubContentFile(
            {
                "content": encodebytes(config.encode()).decode(),
                "path": github_types.GitHubFilePath(".mergify.yml"),
                "type": "file",
                "sha": github_types.SHAType("azertyu"),
            }
        )

    client = mock.Mock()
    client.item.return_value = item()
    fake_repository.installation.client = client

    config_file = await fake_repository.get_mergify_config_file()
    assert config_file is not None

    schema = await mergify_conf.get_mergify_config_from_file(
        mock.MagicMock(), config_file
    )
    assert isinstance(schema, dict)

    assert len(schema["pull_request_rules"].rules) == 1

    comment = schema["pull_request_rules"].rules[0].actions["comment"].config
    assert comment == {"message": "I love Mergify", "bot_account": "foo-bot"}

    rebase = schema["pull_request_rules"].rules[0].actions["rebase"].config
    assert rebase == {"autosquash": True, "bot_account": "test-bot-account"}

    config = """
defaults:
  actions:
    comment:
      message: I love Mergify
      bot_account: AutoBot
pull_request_rules:
  - name: ahah
    conditions:
    - base=main
    actions:
      comment:
        message: I really love Mergify
"""

    client = mock.Mock()
    client.item.return_value = item()
    fake_repository.installation.client = client

    fake_repository._caches.mergify_config_file.delete()
    await fake_repository.clear_config_file_cache(
        fake_repository.installation.redis.cache,
        fake_repository.repo["id"],
    )
    config_file = await fake_repository.get_mergify_config_file()
    assert config_file is not None

    schema = await mergify_conf.get_mergify_config_from_file(
        mock.MagicMock(), config_file
    )
    assert isinstance(schema, dict)

    assert len(schema["pull_request_rules"].rules) == 1

    comment = schema["pull_request_rules"].rules[0].actions["comment"].config
    assert comment == {"message": "I really love Mergify", "bot_account": "AutoBot"}


async def test_get_mergify_config_file_content_from_cache(
    fake_repository: context.Repository,
) -> None:
    config = """
    defaults:
      actions:
        comment:
          message: Hey
          bot_account: foo-bot
        rebase:
          bot_account: test-bot-account
    pull_request_rules:
      - name: ahah
        conditions:
        - base=main
        actions:
          comment:
            message: I love Mergify
          rebase: {}
    """
    client = mock.AsyncMock()
    client.item.side_effect = [
        github_types.GitHubContentFile(
            {
                "content": encodebytes(config.encode()).decode(),
                "type": "file",
                "path": github_types.GitHubFilePath(".github/mergify.yml"),
                "sha": github_types.SHAType("zeazeaze"),
            }
        ),
    ]

    fake_repository.installation.client = client
    await fake_repository.get_mergify_config_file()
    cached_config_file = await fake_repository.get_cached_config_file()
    assert cached_config_file is not None
    assert cached_config_file["content"] == encodebytes(config.encode()).decode()

    await fake_repository.clear_config_file_cache(
        fake_repository.installation.redis.cache,
        fake_repository.repo["id"],
    )

    cached_config_file = await fake_repository.get_cached_config_file()
    assert cached_config_file is None


async def test_get_mergify_config_location_from_cache(
    fake_repository: context.Repository,
) -> None:
    client = mock.AsyncMock()
    client.item.side_effect = [
        http.HTTPNotFound("Not Found", request=mock.Mock(), response=mock.Mock()),
        http.HTTPNotFound("Not Found", request=mock.Mock(), response=mock.Mock()),
        github_types.GitHubContentFile(
            {
                "content": encodebytes(b"whatever").decode(),
                "type": "file",
                "path": github_types.GitHubFilePath(".github/mergify.yml"),
                "sha": github_types.SHAType("zeazeaze"),
            }
        ),
    ]
    fake_repository.installation.client = client
    await fake_repository.get_mergify_config_file()
    assert client.item.call_count == 3
    client.item.assert_has_calls(
        [
            mock.call(
                "/repos/Mergifyio/mergify-engine/contents/.mergify.yml",
                params={"ref": "main"},
            ),
            mock.call(
                "/repos/Mergifyio/mergify-engine/contents/.mergify/config.yml",
                params={"ref": "main"},
            ),
            mock.call(
                "/repos/Mergifyio/mergify-engine/contents/.github/mergify.yml",
                params={"ref": "main"},
            ),
        ]
    )

    client.item.reset_mock()
    client.item.side_effect = [
        github_types.GitHubContentFile(
            {
                "content": encodebytes(b"whatever").decode(),
                "type": "file",
                "path": github_types.GitHubFilePath(".github/mergify.yml"),
                "sha": github_types.SHAType("zeazeaze"),
            }
        ),
    ]

    fake_repository._caches = context.RepositoryCaches()
    await fake_repository.get_mergify_config_file()
    assert client.item.call_count == 0


@pytest.mark.parametrize(
    "invalid",
    (
        (
            """
            pull_request_rules:
              - name: ahah
                conditions:
                actions:
                  coment:
                    message: |
                      This pull request has been approved by
                      {% for name in approved_reviews_by %}
                      @{{ name }}
            """
        ),
    ),
)
async def test_get_mergify_config_invalid(
    invalid: str, fake_repository: context.Repository
) -> None:
    with pytest.raises(mergify_conf.InvalidRules):

        async def item(
            *args: typing.Any, **kwargs: typing.Any
        ) -> github_types.GitHubContentFile:
            return github_types.GitHubContentFile(
                {
                    "content": encodebytes(invalid.encode()).decode(),
                    "path": github_types.GitHubFilePath(".mergify.yml"),
                    "type": "file",
                    "sha": github_types.SHAType("azertyu"),
                }
            )

        client = mock.Mock()
        client.item.return_value = item()
        fake_repository.installation.client = client

        config_file = await fake_repository.get_mergify_config_file()
        assert config_file is not None
        await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), config_file)


def test_user_configuration_schema() -> None:
    with pytest.raises(voluptuous.Invalid) as exc_info:
        rules.UserConfigurationSchema(rules.YamlSchema("- no\n* way"))
    assert str(exc_info.value) == "Invalid YAML at [line 2, column 2]"

    with pytest.raises(voluptuous.Invalid) as i:
        rules.UserConfigurationSchema(
            rules.YamlSchema(
                """
                pull_request_rules:
                  - name: ahah
                    key: not really what we expected
                """
            )
        )
    assert (
        str(i.value) == "extra keys not allowed @ data['pull_request_rules'][0]['key']"
    )

    ir = mergify_conf.InvalidRules(i.value, ".mergify.yml")
    assert str(ir) == (
        "* extra keys not allowed @ pull_request_rules → item 0 → key\n"
        "* required key not provided @ pull_request_rules → item 0 → actions\n"
        "* required key not provided @ pull_request_rules → item 0 → conditions"
    )
    assert [] == ir.get_annotations(".mergify.yml")

    with pytest.raises(voluptuous.Invalid) as i:
        rules.UserConfigurationSchema(
            rules.YamlSchema(
                """invalid:
- *yaml
                """
            )
        )
    assert str(i.value) == "Invalid YAML at [line 2, column 3]"

    ir = mergify_conf.InvalidRules(i.value, ".mergify.yml")
    assert (
        str(ir)
        == """Invalid YAML @ line 2, column 3
```
found undefined alias
  in "<unicode string>", line 2, column 3
```"""
    )
    assert [
        {
            "annotation_level": "failure",
            "end_column": 3,
            "end_line": 2,
            "message": "found undefined alias\n"
            '  in "<unicode string>", line 2, column 3',
            "path": github_types.GitHubFilePath(".mergify.yml"),
            "start_column": 3,
            "start_line": 2,
            "title": "Invalid YAML",
        }
    ] == ir.get_annotations(".mergify.yml")

    with pytest.raises(voluptuous.Invalid) as i:
        rules.UserConfigurationSchema(
            rules.YamlSchema(
                """
                pull_request_rules:
                """
            )
        )
    assert (
        str(i.value)
        == "expected a list for dictionary value @ data['pull_request_rules']"
    )
    ir = mergify_conf.InvalidRules(i.value, ".mergify.yml")
    assert str(ir) == "expected a list for dictionary value @ pull_request_rules"

    with pytest.raises(voluptuous.Invalid) as i:
        rules.UserConfigurationSchema(rules.YamlSchema(""))
    assert str(i.value) == "expected a dictionary"
    ir = mergify_conf.InvalidRules(i.value, ".mergify.yml")
    assert str(ir) == "expected a dictionary"

    with pytest.raises(voluptuous.Invalid) as i:
        rules.UserConfigurationSchema(
            rules.YamlSchema(
                """
                pull_request_rules:
                  - name: add label
                    conditions:
                      - conflict
                    actions:
                      label:
                        add:
                          - conflict:
                """
            )
        )
    assert (
        str(i.value)
        == "expected str @ data['pull_request_rules'][0]['actions']['label']['add'][0]"
    )
    ir = mergify_conf.InvalidRules(i.value, ".mergify.yml")
    assert (
        str(ir)
        == "expected str @ pull_request_rules → item 0 → actions → label → add → item 0"
    )

    # Just to make sure both version works
    rules.UserConfigurationSchema(
        rules.YamlSchema(
            """
            queue_rules:
              - name: foo
                merge_conditions:
                  - "#approved-reviews-by>=2"
                  - check-success=Travis CI - Pull Request
            """
        )
    )
    rules.UserConfigurationSchema(
        rules.YamlSchema(
            """
            queue_rules:
              - name: foo
                conditions:
                  - "#approved-reviews-by>=2"
                  - check-success=Travis CI - Pull Request
            """
        )
    )

    with pytest.raises(voluptuous.error.MultipleInvalid) as exc:
        rules.UserConfigurationSchema(
            rules.YamlSchema(
                """
                queue_rules:
                  - name: foo
                    merge_conditions:
                      - base=test
                    conditions:
                      - "#approved-reviews-by>=2"
                      - check-success=Travis CI - Pull Request
                """
            )
        )

    assert str(exc.value).startswith(
        "Cannot have both `conditions` and `merge_conditions`, only use `merge_conditions (`conditions` is deprecated)"
    )

    with pytest.raises(voluptuous.error.MultipleInvalid) as exc:
        rules.UserConfigurationSchema(
            rules.YamlSchema(
                """
                queue_rules:
                  - name: foo
                    routing_conditions:
                      - base=test
                    queue_conditions:
                      - "#approved-reviews-by>=2"
                      - check-success=Travis CI - Pull Request
                """
            )
        )

    assert str(exc.value).startswith(
        "Cannot have both `routing_conditions` and `queue_conditions`, only use `queue_conditions (`routing_conditions` is deprecated)"
    )

    # Just to make sure both routing_conditions and queue_conditions works fine
    # on their own
    rules.UserConfigurationSchema(
        rules.YamlSchema(
            """
            queue_rules:
              - name: foo
                routing_conditions:
                  - base=test
                  - "#approved-reviews-by>=2"
                  - check-success=Travis CI - Pull Request
            """
        )
    )

    rules.UserConfigurationSchema(
        rules.YamlSchema(
            """
            queue_rules:
              - name: foo
                queue_conditions:
                  - base=test
                  - "#approved-reviews-by>=2"
                  - check-success=Travis CI - Pull Request
            """
        )
    )

    with pytest.raises(voluptuous.error.MultipleInvalid) as exc:
        rules.UserConfigurationSchema(
            rules.YamlSchema(
                """
                pull_request_rules:
                  - name: queue_pull_requests
                    conditions:
                      - base=main
                    actions:
                      queue:
                        name: default
                        merge_method: rebase
                        method: rebase
                """
            )
        )

    assert str(exc.value).startswith(
        "Cannot have both `method` and `merge_method` options in `queue` action, use `merge_method` only (`method` is deprecated)"
    )

    # new `merge_method` option name
    rules.UserConfigurationSchema(
        rules.YamlSchema(
            """
            pull_request_rules:
              - name: queue_pull_requests
                conditions:
                  - base=main
                actions:
                  queue:
                    name: default
                    merge_method: rebase
            """
        )
    )

    # old `method` option name for retro-compatibility
    validated_config = rules.UserConfigurationSchema(
        rules.YamlSchema(
            """
            pull_request_rules:
              - name: queue_pull_requests
                conditions:
                  - base=main
                actions:
                  queue:
                    name: default
                    method: fast-forward
            """
        )
    )
    assert (
        validated_config["pull_request_rules"]
        .rules[0]
        .actions["queue"]
        .config["merge_method"]
        == "fast-forward"
    )


@pytest.mark.parametrize(
    "extends",
    [
        (".github"),
        ("repo_name"),
    ],
)
def test_extends_parsing(extends: str) -> None:
    schema = rules.UserConfigurationSchema({"extends": extends})
    assert schema["extends"] == extends


@pytest.mark.parametrize(
    "extends",
    [
        "",
    ],
)
def test_extends_ko(extends: str | None) -> None:
    with pytest.raises(voluptuous.Invalid) as i:
        rules.UserConfigurationSchema({"extends": extends})
    assert str(i.value) == "not a valid value for dictionary value @ data['extends']"


async def test_extends_infinite_loop() -> None:
    mergify_config = mergify_conf.MergifyConfig(
        {
            "extends": github_types.GitHubRepositoryName(".github"),
            "pull_request_rules": prr_config.PullRequestRules([]),
            "queue_rules": qr_config.QueueRules([]),
            "partition_rules": partr_config.PartitionRules([]),
            "defaults": mergify_conf.Defaults({"actions": {}}),
            "commands_restrictions": {},
            "raw_config": {
                "extends": github_types.GitHubRepositoryName(".github"),
            },
        }
    )
    repository_ctxt = mock.MagicMock()
    repository_ctxt.get_mergify_config.side_effect = mock.AsyncMock(
        return_value=mergify_config
    )

    repository_ctxt.installation.get_repository_by_name = mock.AsyncMock(
        return_value=repository_ctxt
    )
    with pytest.raises(mergify_conf.InvalidRules) as i:
        await mergify_conf.get_mergify_config_from_dict(
            repository_ctxt, mergify_config["raw_config"], ""
        )
    assert (
        str(i.value)
        == "Only configuration from other repositories can be extended. @ extends"
    )


async def test_extends_limit(
    fake_repository: context.Repository, respx_mock: respx.MockRouter
) -> None:
    mergify_config = mergify_conf.MergifyConfig(
        {
            "extends": github_types.GitHubRepositoryName(".github"),
            "pull_request_rules": prr_config.PullRequestRules([]),
            "queue_rules": qr_config.QueueRules([]),
            "partition_rules": partr_config.PartitionRules([]),
            "defaults": mergify_conf.Defaults({"actions": {}}),
            "commands_restrictions": {},
            "raw_config": {
                "extends": github_types.GitHubRepositoryName(".github"),
            },
        }
    )

    # "Mergifyio/mergify-engine" because this is the value of the fake_repository
    # that is returned by the mocked `get_repository_by_name`
    respx_mock.get("/repos/Mergifyio/mergify-engine/installation").respond(200)

    repository_ctxt = mock.MagicMock()
    repository_ctxt.installation.get_repository_by_name = mock.AsyncMock(
        return_value=fake_repository
    )
    with mock.patch.object(
        fake_repository,
        "get_mergify_config_file",
        side_effect=mock.AsyncMock(
            return_value=context.MergifyConfigFile(
                type="file",
                content="whatever",
                sha=github_types.SHAType("azertyuiop"),
                path=github_types.GitHubFilePath("whatever"),
                decoded_content="extends: faraway",
            )
        ),
    ):
        with pytest.raises(mergify_conf.InvalidRules) as i:
            await mergify_conf.get_mergify_config_from_dict(
                repository_ctxt, mergify_config["raw_config"], ""
            )
    assert (
        str(i.value)
        == "Maximum number of extended configuration reached. Limit is 1. @ extends"
    )


@pytest.mark.respx(base_url=settings.GITHUB_REST_API_URL)
async def test_extends_without_database_cache(
    fake_repository: context.Repository,
    respx_mock: respx.MockRouter,
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    # Config extension containing default values to assert the extension worked
    mergify_config_extension = mergify_conf.MergifyConfig(
        {
            "extends": None,
            "pull_request_rules": prr_config.PullRequestRules([]),
            "queue_rules": qr_config.QueueRules([]),
            "partition_rules": partr_config.PartitionRules([]),
            "defaults": mergify_conf.Defaults({"actions": {"hello": {}}}),
            "commands_restrictions": {},
            "raw_config": {},
        }
    )

    respx_mock.get("repos/Mergifyio/.github").respond(
        200,
        json={
            "id": 1,
            "name": ".github",
            "owner": fake_repository.repo["owner"],
            "private": True,
            "default_branch": "main",
            "full_name": "",
        },
    )
    respx_mock.get("repos/Mergifyio/.github/installation").respond(200)

    with mock.patch.object(
        context.Repository,
        "get_mergify_config",
        mock.AsyncMock(return_value=mergify_config_extension),
    ):
        config = await mergify_conf.get_mergify_config_from_dict(
            fake_repository,
            {"extends": github_types.GitHubRepositoryName(".github")},
            "",
        )

    # Configuration has been extended and have defaults from the extension
    assert config["extends"] == ".github"
    assert config["defaults"] == {"actions": {"hello": {}}}
    result = await db.scalars(sqlalchemy.select(github_repository.GitHubRepository))
    repos = list(result)
    assert len(repos) == 1
    assert repos[0].id == 1
    assert repos[0].name == ".github"


@pytest.mark.respx(base_url=settings.GITHUB_REST_API_URL)
async def test_extends_with_database_cache(
    fake_repository: context.Repository,
    respx_mock: respx.MockRouter,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    redis_links: redis_utils.RedisLinks,
) -> None:
    # Insert the repo containing the configuration extension
    repo = await github_repository.GitHubRepository.get_or_create(
        db,
        {
            "id": github_types.GitHubRepositoryIdType(1),
            "name": github_types.GitHubRepositoryName(".github"),
            "owner": fake_repository.repo["owner"],
            "private": True,
            "default_branch": github_types.GitHubRefType("main"),
            "full_name": f"{fake_repository.repo['owner']['login']}/.github",
            "archived": False,
        },
    )
    db.add(repo)
    await db.commit()

    await redis_links.cache.set(
        f"mergify-installation/{fake_repository.repo['owner']['login']}/.github",
        json.dumps({"installed": True, "error": None}),
    )

    # Config extension containing default values to assert the extension worked
    mergify_config_extension = mergify_conf.MergifyConfig(
        {
            "extends": None,
            "pull_request_rules": prr_config.PullRequestRules([]),
            "queue_rules": qr_config.QueueRules([]),
            "partition_rules": partr_config.PartitionRules([]),
            "defaults": mergify_conf.Defaults({"actions": {"hello": {}}}),
            "commands_restrictions": {},
            "raw_config": {},
        }
    )

    with mock.patch.object(
        context.Repository,
        "get_mergify_config",
        mock.AsyncMock(return_value=mergify_config_extension),
    ):
        config = await mergify_conf.get_mergify_config_from_dict(
            fake_repository,
            {"extends": github_types.GitHubRepositoryName(".github")},
            "",
        )

    # No HTTP request to GH
    respx_mock.calls.assert_not_called()
    # Configuration has been extended and have defaults from the extension
    assert config["extends"] == ".github"
    assert config["defaults"] == {"actions": {"hello": {}}}


def test_user_binary_file() -> None:
    with pytest.raises(voluptuous.Invalid) as i:
        rules.UserConfigurationSchema(rules.YamlSchema(chr(4)))
    assert str(i.value) == "Invalid YAML at []"
    ir = mergify_conf.InvalidRules(i.value, ".mergify.yml")
    assert (
        str(ir)
        == """Invalid YAML
```
unacceptable character #x0004: control characters are not allowed
  in "<unicode string>", position 0
```"""
    )
    assert ir.get_annotations(".mergify.yml") == []


@pytest.mark.parametrize(
    "invalid,match",
    (
        (
            {"name": "hello", "conditions": ["this is wrong"], "actions": {}},
            "Invalid condition ",
        ),
        (
            {"name": "invalid regexp", "conditions": ["head~=(lol"], "actions": {}},
            r"Invalid condition 'head~=\(lol'. Invalid regular expression: "
            r"missing \), "
            r"unterminated subpattern at position 0 @ ",
        ),
        (
            {"name": "hello", "conditions": ["head|4"], "actions": {}},
            "Invalid condition ",
        ),
        (
            {"name": "hello", "conditions": [{"foo": "bar"}], "actions": {}},
            r"extra keys not allowed @ data\[0\]\['conditions'\]\[0\]\['foo'\]",
        ),
        (
            {"name": "hello", "conditions": [], "actions": {}, "foobar": True},
            "extra keys not allowed",
        ),
        (
            {"name": "hello", "conditions": [], "actions": {"merge": True}},
            r"expected a dictionary for dictionary value "
            r"@ data\[0\]\['actions'\]\['merge'\]",
        ),
        (
            {
                "name": "hello",
                "conditions": [],
                "actions": {"backport": {"regexes": ["(azerty"]}},
            },
            r"missing \), unterminated subpattern at position 0 "
            r"@ data\[0\]\['actions'\]\['backport'\]\['regexes'\]\[0\]",
        ),
        (
            {"name": "hello", "conditions": [], "actions": {"backport": True}},
            r"expected a dictionary for dictionary value "
            r"@ data\[0\]\['actions'\]\['backport'\]",
        ),
        (
            {
                "name": "hello",
                "conditions": [],
                "actions": {"review": {"message": "{{syntax error"}},
            },
            r"Template syntax error @ data\[0\]\['actions'\]\['review'\]\['message'\]\[line 1\]",
        ),
        (
            {
                "name": "hello",
                "conditions": [],
                "actions": {"review": {"message": "{{unknownattribute}}"}},
            },
            r"Template syntax error for dictionary value @ data\[0\]\['actions'\]\['review'\]\['message'\]",
        ),
    ),
)
def test_pull_request_rule_schema_invalid(
    invalid: typing.Any, match: re.Pattern[str]
) -> None:
    with pytest.raises(voluptuous.MultipleInvalid, match=match):
        pull_request_rule_from_list([invalid])


async def test_get_pull_request_rules_evaluator(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    client = mock.Mock()

    get_reviews = [
        {
            "user": {"login": "sileht", "id": 12321, "type": "User"},
            "state": "APPROVED",
            "author_association": "MEMBER",
        }
    ]
    get_files = [{"filename": "README.rst"}, {"filename": "setup.py"}]
    get_team_members = [{"login": "sileht", "id": 12321}, {"login": "jd", "id": 2644}]

    get_checks: list[github_types.GitHubCheckRun] = []
    get_statuses: list[github_types.GitHubStatus] = [
        {
            "context": "continuous-integration/fake-ci",
            "state": "success",
            "description": "foobar",
            "target_url": "http://example.com",
            "avatar_url": "",
        }
    ]

    async def client_item(
        url: str, *args: typing.Any, **kwargs: typing.Any
    ) -> dict[str, str] | None:
        if url == "/repos/Mergifyio/mergify-engine/collaborators/sileht/permission":
            return {"permission": "write"}

        if url == "/repos/Mergifyio/mergify-engine/collaborators/jd/permission":
            return {"permission": "write"}

        if url == "/repos/Mergifyio/mergify-engine/branches/main/protection":
            raise http.HTTPNotFound(
                message="boom", response=mock.Mock(), request=mock.Mock()
            )

        raise RuntimeError(f"not handled url {url}")

    client.item.side_effect = client_item

    async def client_items(
        url: str, *args: typing.Any, **kwargs: typing.Any
    ) -> abc.AsyncGenerator[dict[str, typing.Any], None] | None:
        if url == "/repos/Mergifyio/mergify-engine/pulls/1/reviews":
            for r in get_reviews:
                yield r
        elif url == "/repos/Mergifyio/mergify-engine/pulls/1/files":
            for f in get_files:
                yield f
        elif url == "/repos/Mergifyio/mergify-engine/commits/the-head-sha/check-runs":
            for c in get_checks:
                yield c
        elif url == "/repos/Mergifyio/mergify-engine/commits/the-head-sha/status":
            for s in get_statuses:
                yield s
        elif url == "/orgs/Mergifyio/teams/my-reviewers/members":
            for tm in get_team_members:
                yield tm
        else:
            raise RuntimeError(f"not handled url {url}")

    client.items.side_effect = client_items

    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client

    # Empty conditions
    pull_request_rules = prr_config.PullRequestRules(
        [
            prr_config.PullRequestRule(
                name=prr_config.PullRequestRuleName("default"),
                disabled=None,
                conditions=conditions.PullRequestRuleConditions([]),
                actions={},
                hidden=False,
            )
        ]
    )

    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
    assert [r.name for r in match.rules] == ["default"]
    assert [r.name for r in match.matching_rules] == ["default"]
    for rule in match.rules:
        assert rule.actions == {}

    pull_request_rules = pull_request_rule_from_list(
        [{"name": "hello", "conditions": ["base:main"], "actions": {}}]
    )

    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
    assert [r.name for r in match.rules] == ["hello"]
    assert [r.name for r in match.matching_rules] == ["hello"]
    for rule in match.rules:
        assert rule.actions == {}

    pull_request_rules = pull_request_rule_from_list(
        [
            {"name": "hello", "conditions": ["base:main"], "actions": {}},
            {"name": "backport", "conditions": ["base:main"], "actions": {}},
        ]
    )

    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
    assert [r.name for r in match.rules] == ["hello", "backport"]
    assert [r.name for r in match.matching_rules] == ["hello", "backport"]
    for rule in match.rules:
        assert rule.actions == {}

    pull_request_rules = pull_request_rule_from_list(
        [
            {"name": "hello", "conditions": ["author:foobar"], "actions": {}},
            {"name": "backport", "conditions": ["base:main"], "actions": {}},
        ]
    )

    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
    assert [r.name for r in match.rules] == ["hello", "backport"]
    assert [r.name for r in match.matching_rules] == ["backport"]
    for rule in match.rules:
        assert rule.actions == {}

    pull_request_rules = pull_request_rule_from_list(
        [
            {"name": "hello", "conditions": ["author:contributor"], "actions": {}},
            {"name": "backport", "conditions": ["base:main"], "actions": {}},
        ]
    )

    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
    assert [r.name for r in match.rules] == ["hello", "backport"]
    assert [r.name for r in match.matching_rules] == ["hello", "backport"]
    for rule in match.rules:
        assert rule.actions == {}

    # No match
    pull_request_rules = pull_request_rule_from_list(
        [
            {
                "name": "merge",
                "conditions": [
                    "base=xyz",
                    "check-success=continuous-integration/fake-ci",
                    "#approved-reviews-by>=1",
                ],
                "actions": {},
            }
        ]
    )

    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
    assert [r.name for r in match.rules] == ["merge"]
    assert [r.name for r in match.not_applicable_base_changeable_attributes_rules] == [
        "merge"
    ]

    pull_request_rules = pull_request_rule_from_list(
        [
            {
                "name": "merge",
                "conditions": [
                    "base=main",
                    "check-success=continuous-integration/fake-ci",
                    "#approved-reviews-by>=1",
                ],
                "actions": {},
            }
        ]
    )

    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
    assert [r.name for r in match.rules] == ["merge"]
    assert [r.name for r in match.matching_rules] == ["merge"]
    for rule in match.rules:
        assert rule.actions == {}

    pull_request_rules = pull_request_rule_from_list(
        [
            {
                "name": "merge",
                "conditions": [
                    "base=main",
                    "check-success=continuous-integration/fake-ci",
                    "#approved-reviews-by>=2",
                ],
                "actions": {},
            },
            {
                "name": "fast merge",
                "conditions": [
                    "base=main",
                    "label=fast-track",
                    "check-success=continuous-integration/fake-ci",
                    "#approved-reviews-by>=1",
                ],
                "actions": {},
            },
            {
                "name": "fast merge with alternate ci",
                "conditions": [
                    "base=main",
                    {
                        "or": [
                            {
                                "and": [
                                    "label=automerge",
                                    "label=ready",
                                ]
                            },
                            "label=fast-track",
                        ]
                    },
                    "check-success=continuous-integration/fake-ci-bis",
                    "#approved-reviews-by>=1",
                ],
                "actions": {},
            },
            {
                "name": "fast merge from a bot",
                "conditions": [
                    "base=main",
                    {
                        "or": [
                            "label=python-deps",
                            "label=node-deps",
                        ]
                    },
                    "author=mybot",
                    "check-success=continuous-integration/fake-ci",
                ],
                "actions": {},
            },
        ]
    )
    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)

    assert [r.name for r in match.rules] == [
        "merge",
        "fast merge",
        "fast merge with alternate ci",
        "fast merge from a bot",
    ]
    assert [r.name for r in match.matching_rules] == [
        "merge",
        "fast merge",
        "fast merge with alternate ci",
    ]
    for rule in match.rules:
        assert rule.actions == {}

    assert match.matching_rules[0].name == "merge"
    assert not match.matching_rules[0].conditions.match
    missing_conditions = [
        c for c in match.matching_rules[0].conditions.walk() if not c.match
    ]
    assert len(missing_conditions) == 1
    assert str(missing_conditions[0]) == "#approved-reviews-by>=2"

    assert match.matching_rules[1].name == "fast merge"
    assert not match.matching_rules[1].conditions.match
    missing_conditions = [
        c for c in match.matching_rules[1].conditions.walk() if not c.match
    ]
    assert len(missing_conditions) == 1
    assert str(missing_conditions[0]) == "label=fast-track"

    assert match.matching_rules[2].name == "fast merge with alternate ci"
    assert not match.matching_rules[2].conditions.match
    missing_conditions = [
        c for c in match.matching_rules[2].conditions.walk() if not c.match
    ]
    assert len(missing_conditions) == 4
    assert (
        str(missing_conditions[0]) == "check-success=continuous-integration/fake-ci-bis"
    )
    assert str(missing_conditions[1]) == "label=fast-track"
    assert str(missing_conditions[2]) == "label=automerge"
    assert str(missing_conditions[3]) == "label=ready"

    # Team conditions with one review missing
    pull_request_rules = pull_request_rule_from_list(
        [
            {
                "name": "default",
                "conditions": [
                    "approved-reviews-by=@Mergifyio/my-reviewers",
                    "#approved-reviews-by>=2",
                ],
                "actions": {},
            }
        ]
    )

    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
    assert [r.name for r in match.rules] == ["default"]
    assert [r.name for r in match.matching_rules] == ["default"]

    assert match.matching_rules[0].name == "default"
    assert not match.matching_rules[0].conditions.match
    missing_conditions = [
        c for c in match.matching_rules[0].conditions.walk() if not c.match
    ]
    assert len(missing_conditions) == 1
    assert str(missing_conditions[0]) == "#approved-reviews-by>=2"

    get_reviews.append(
        {
            "user": {"login": "jd", "id": 2644, "type": "User"},
            "state": "APPROVED",
            "author_association": "MEMBER",
        }
    )

    ctxt._caches.reviews.delete()
    ctxt._caches.consolidated_reviews.delete()

    # Team conditions with no review missing
    pull_request_rules = pull_request_rule_from_list(
        [
            {
                "name": "default",
                "conditions": [
                    "approved-reviews-by=@Mergifyio/my-reviewers",
                    "#approved-reviews-by>=2",
                ],
                "actions": {},
            }
        ]
    )

    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
    assert [r.name for r in match.rules] == ["default"]
    assert [r.name for r in match.matching_rules] == ["default"]

    assert match.matching_rules[0].name == "default"
    assert match.matching_rules[0].conditions.match

    # Forbidden labels, when no label set
    pull_request_rules = pull_request_rule_from_list(
        [
            {
                "name": "default",
                "conditions": ["-label~=^(status/wip|status/blocked|review/need2)$"],
                "actions": {},
            }
        ]
    )

    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
    assert [r.name for r in match.rules] == ["default"]
    assert [r.name for r in match.matching_rules] == ["default"]
    assert match.matching_rules[0].name == "default"
    assert match.matching_rules[0].conditions.match

    # Forbidden labels, when forbiden label set
    ctxt.pull["labels"] = [
        {"id": 0, "color": "#1234", "default": False, "name": "status/wip"}
    ]

    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
    assert [r.name for r in match.rules] == ["default"]
    assert [r.name for r in match.matching_rules] == ["default"]
    assert match.matching_rules[0].name == "default"
    assert not match.matching_rules[0].conditions.match
    missing_conditions = [
        c for c in match.matching_rules[0].conditions.walk() if not c.match
    ]
    assert len(missing_conditions) == 1
    assert str(missing_conditions[0]) == (
        "-label~=^(status/wip|status/blocked|review/need2)$"
    )

    # Forbidden labels, when other label set
    ctxt.pull["labels"] = [
        {"id": 0, "color": "#1234", "default": False, "name": "allowed"}
    ]

    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
    assert [r.name for r in match.rules] == ["default"]
    assert [r.name for r in match.matching_rules] == ["default"]
    assert match.matching_rules[0].name == "default"
    assert match.matching_rules[0].conditions.match
    missing_conditions = [
        c for c in match.matching_rules[0].conditions.walk() if not c.match
    ]
    assert len(missing_conditions) == 0

    # Test team expander
    pull_request_rules = pull_request_rule_from_list(
        [
            {
                "name": "default",
                "conditions": ["author~=^(user1|user2|contributor)$"],
                "actions": {},
            }
        ]
    )
    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
    assert [r.name for r in match.rules] == ["default"]
    assert [r.name for r in match.matching_rules] == ["default"]
    assert match.matching_rules[0].name == "default"
    assert match.matching_rules[0].conditions.match
    missing_conditions = [
        c for c in match.matching_rules[0].conditions.walk() if not c.match
    ]

    # branch protection
    async def client_item_with_branch_protection_enabled(
        url: str, *args: typing.Any, **kwargs: typing.Any
    ) -> None | (dict[str, dict[str, list[str] | bool]]):
        if url == "/repos/Mergifyio/mergify-engine/branches/main/protection":
            return {
                "required_status_checks": {"contexts": ["awesome-ci"], "strict": False},
                "required_linear_history": {"enabled": False},
            }
        raise RuntimeError(f"not handled url {url}")

    client.item.side_effect = client_item_with_branch_protection_enabled
    ctxt.repository._caches.branch_protections.clear()
    pull_request_rules = pull_request_rule_from_list(
        [
            {
                "name": "default",
                "conditions": [],
                "actions": {"merge": {}, "comment": {"message": "yo"}},
            }
        ]
    )
    match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)

    assert [r.name for r in match.rules] == ["default", "default"]
    assert list(match.matching_rules[0].actions.keys()) == ["merge"]
    assert len(match.matching_rules[0].conditions.condition.conditions) == 3
    assert not match.matching_rules[0].conditions.match
    config_change = match.matching_rules[0].conditions.condition.conditions[0]
    assert isinstance(config_change, rules.conditions.RuleCondition)
    assert str(config_change) == "-mergify-configuration-changed"
    assert (
        config_change.description
        == ":pushpin: merge -> allow_merging_configuration_change setting requirement"
    )
    draft = match.matching_rules[0].conditions.condition.conditions[1]
    assert isinstance(draft, rules.conditions.RuleCondition)
    assert str(draft) == "-draft"
    assert draft.description == ":pushpin: merge requirement"
    group = match.matching_rules[0].conditions.condition.conditions[2]
    assert isinstance(group, rules.conditions.RuleConditionCombination)
    assert group.operator == "or"
    assert len(group.conditions) == 3
    assert str(group.conditions[0]) == "check-success=awesome-ci"
    assert str(group.conditions[1]) == "check-neutral=awesome-ci"
    assert str(group.conditions[2]) == "check-skipped=awesome-ci"
    missing_conditions = [
        c for c in match.matching_rules[0].conditions.walk() if not c.match
    ]
    assert len(missing_conditions) == 3
    assert str(missing_conditions[0]) == "check-neutral=awesome-ci"
    assert str(missing_conditions[1]) == "check-skipped=awesome-ci"
    assert str(missing_conditions[2]) == "check-success=awesome-ci"
    assert list(match.matching_rules[1].actions.keys()) == ["comment"]
    assert len(match.matching_rules[1].conditions.condition.conditions) == 0


def test_check_runs_custom() -> None:
    pull_request_rules = rules.UserConfigurationSchema(
        rules.YamlSchema(
            """
            pull_request_rules:
              - name: ahah
                conditions:
                - base=main
                actions:
                  post_check:
                    title: '{{ check_rule_name }} whatever'
                    summary: |
                      This pull request has been checked!
                      Thank you @{{author}} for your contributions!

                      {{ check_conditions }}
            """
        )
    )["pull_request_rules"]
    assert [rule.name for rule in pull_request_rules] == [
        "ahah",
    ]


def test_check_runs_default() -> None:
    pull_request_rules = rules.UserConfigurationSchema(
        rules.YamlSchema(
            """
            pull_request_rules:
              - name: ahah
                conditions:
                - base=main
                actions:
                  post_check: {}
            """
        )
    )["pull_request_rules"]
    assert [rule.name for rule in pull_request_rules] == [
        "ahah",
    ]


def test_merge_config() -> None:
    config: dict[str, typing.Any] = {
        "defaults": {"actions": {"rebase": {"bot_account": "foo"}}},
        "pull_request_rules": [
            {
                "name": "hello",
                "conditions": ["head:main"],
                "actions": {"rebase": {}},
            }
        ],
    }

    defaults = config.pop("defaults")
    expected_config = copy.deepcopy(config)
    expected_config["pull_request_rules"][0]["actions"].update(defaults["actions"])

    mergify_conf.merge_config_with_defaults(config, defaults)
    assert config == expected_config

    config = {
        "defaults": {
            "actions": {
                "rebase": {"bot_account": "foo"},
                "comment": {"message": "Hello World!"},
            }
        },
        "pull_request_rules": [
            {
                "name": "hello",
                "conditions": ["head:main"],
                "actions": {"rebase": {"bot_account": "bar"}},
            }
        ],
    }

    defaults = config.pop("defaults")
    expected_config = copy.deepcopy(config)
    mergify_conf.merge_config_with_defaults(config, defaults)
    assert config == expected_config

    config = {
        "pull_request_rules": [
            {
                "name": "hello",
                "conditions": ["head:main"],
                "actions": {"rebase": {"bot_account": "bar"}},
            }
        ],
    }
    defaults = config.pop("defaults", {})
    expected_config = copy.deepcopy(config)
    mergify_conf.merge_config_with_defaults(config, defaults)
    assert config == expected_config


async def test_actions_with_options_none() -> None:
    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content="""
defaults:
  actions:
    post_check:
    rebase:
    queue:
      name: default
    comment:
      message: Hey
      bot_account: "foobar"
queue_rules:
    - name: default
      conditions: []
pull_request_rules:
  - name: ahah
    conditions:
    - base=main
    actions:
      comment:
        message: "foobar"
      rebase:
        bot_account: "foobar"
      post_check:
      queue:
            """,
    )

    config = await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)

    assert next(list(rule.actions.keys()) for rule in config["pull_request_rules"]) == [
        "comment",
        "rebase",
        "post_check",
        "queue",
    ]


async def test_action_queue_with_duplicate_queue() -> None:
    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content="""
queue_rules:
  - name: default
    conditions: []
  - name: default
    conditions: []
pull_request_rules:
  - name: ahah
    conditions:
    - base=main
    actions:
      queue:
        name: default
""",
    )

    with pytest.raises(mergify_conf.InvalidRules) as e:
        await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)

    assert (
        str(e.value.error)
        == "queue_rules names must be unique, found `default` twice for dictionary value @ data['queue_rules']"
    )


async def test_action_queue_with_no_default_queue() -> None:
    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content="""
pull_request_rules:
  - name: ahah
    conditions:
    - base=main
    actions:
      queue:
        name: missing
            """,
    )

    with pytest.raises(mergify_conf.InvalidRules) as e:
        await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)

    assert str(e.value.error) == "`missing` queue not found"


async def test_default_with_no_pull_requests_rules() -> None:
    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content="""
defaults:
  actions:
    merge:
       merge_bot_account: "bot"
""",
    )

    assert await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)
    config = await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)

    assert config["pull_request_rules"].rules == []


async def test_multiple_cascaded_errors() -> None:
    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content="""
pull_request_rules:
  - name: automatic merge for Dependabot pull requests
    conditions:
    - author=dependabot[bot]
      - status-success=Travis CI - Pull Request
    actions:
    merge:
        method: merge
""",
    )

    with pytest.raises(mergify_conf.InvalidRules) as e:
        await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)

    assert (
        str(e.value)
        == """* Invalid condition 'author=dependabot[bot] - status-success=Travis CI - Pull Request'. Invalid GitHub login @ pull_request_rules → item 0 → conditions → item 0
```
Invalid GitHub login
```
* expected a dictionary for dictionary value @ pull_request_rules → item 0 → actions
* extra keys not allowed @ pull_request_rules → item 0 → merge"""
    )


async def test_queue_action_defaults() -> None:
    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content="""
defaults:
  actions:
    queue:
      method: squash

queue_rules:
- name: default
  conditions: []

pull_request_rules:
- name: ahah
  conditions: []
  actions:
    queue:
      name: default
""",
    )

    config = await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)

    pull_request_rules = list(config["pull_request_rules"])
    assert pull_request_rules[0].actions["queue"].config["name"] == "default"
    assert pull_request_rules[0].actions["queue"].config["merge_method"] == "squash"


def queue_rule_from_list(lst: typing.Any) -> prr_config.PullRequestRules:
    return typing.cast(
        prr_config.PullRequestRules,
        voluptuous.Schema(prr_config.get_pull_request_rules_schema())(lst),
    )


async def test_invalid_disallow_checks_interruption_from_queues() -> None:
    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content="""
queue_rules:
- name: default
  conditions: []
  disallow_checks_interruption_from_queues:
    - whatever
""",
    )

    with pytest.raises(mergify_conf.InvalidRules) as i:
        await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)
    assert (
        "disallow_checks_interruption_from_queues contains an unkown queue: whatever"
        in str(i.value)
    )


async def test_valid_disallow_checks_interruption_from_queues() -> None:
    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content="""
queue_rules:
- name: default
  conditions: []
- name: low
  conditions: []
  disallow_checks_interruption_from_queues:
    - default
""",
    )

    config = await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)
    assert config["queue_rules"][qr_config.QueueName("low")].config[
        "disallow_checks_interruption_from_queues"
    ] == ["default"]


async def test_invalid_interval() -> None:
    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content="""
queue_rules:
- name: default
  conditions: []
  checks_timeout: whatever
  batch_max_wait_time: whatever
""",
    )

    with pytest.raises(mergify_conf.InvalidRules) as i:
        await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)
    assert (
        "* Invalid date interval for dictionary value @ queue_rules → item 0 → batch_max_wait_time"
        in str(i.value)
    )
    assert (
        "* not a valid value for dictionary value @ queue_rules → item 0 → checks_timeout"
        in str(i.value)
    )

    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content="""
queue_rules:
- name: default
  conditions: []
  checks_timeout: 2
  batch_max_wait_time: 2
""",
    )

    with pytest.raises(mergify_conf.InvalidRules) as i:
        await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)
    assert (
        "* expected str for dictionary value @ queue_rules → item 0 → batch_max_wait_time"
        in str(i.value)
    )
    assert (
        "* not a valid value for dictionary value @ queue_rules → item 0 → checks_timeout"
        in str(i.value)
    )


async def test_template_with_empty_body() -> None:
    config = await utils.load_mergify_config(
        """pull_request_rules:
  - name: rule name
    conditions:
      - base=master
    actions:
      queue:
        name: default
        commit_message_template: |
          {{title}}

          {{ body.split('----PR MESSAGE----')[0] }}
"""
    )

    queue_config = config["pull_request_rules"].rules[0].actions["queue"]
    assert (
        queue_config.config["commit_message_template"]
        == "{{title}}\n\n{{ body.split('----PR MESSAGE----')[0] }}\n"
    )


async def test_ignorable_root_key() -> None:
    config = await utils.load_mergify_config(
        """
shared:
  conditions: &cond1
    - status-success=continuous-integration/fake-ci
  conditions: &cond2
    - base=main

queue_rules:
  - name: default
    conditions: *cond1
    speculative_checks: 5
    allow_inplace_checks: true

pull_request_rules:
  - name: Merge me
    conditions: *cond2
    actions:
      queue:
        name: default
""",
    )

    assert "shared" not in config

    queue_condition = config["queue_rules"].rules[0].conditions.condition.conditions[0]
    assert isinstance(queue_condition, conditions.RuleCondition)
    assert str(queue_condition) == "status-success=continuous-integration/fake-ci"

    pull_request_condition = (
        config["pull_request_rules"].rules[0].conditions.condition.conditions[0]
    )
    assert isinstance(pull_request_condition, conditions.RuleCondition)
    assert str(pull_request_condition) == "base=main"


async def test_rule_condition_negation_extract_raw_filter_tree() -> None:
    rule_condition_negation = cond_config.RuleConditionSchema(
        {"not": {"or": ["base=main", "label=foo"]}}
    )
    pr_conditions = conditions.PullRequestRuleConditions([rule_condition_negation])

    expected_tree = {
        "and": [
            {
                "not": {
                    "or": [
                        {"=": ("base", "main")},
                        {"=": ("label", "foo")},
                    ]
                }
            }
        ]
    }

    assert pr_conditions.extract_raw_filter_tree() == expected_tree


async def test_command_only_conditions(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    with pytest.raises(mergify_conf.InvalidRules) as e:
        await utils.load_mergify_config(
            """
queue_rules:
   - name: foo
     conditions:
      - sender=foobar
      - sender-permission=write

pull_request_rules:
   - name: foo
     conditions:
      - sender=foobar
      - sender-permission=write
     actions:
       comment:
"""
        )
    assert (
        str(e.value)
        == """
* Invalid condition 'sender-permission=write'. Attribute only allowed in commands_restrictions section @ pull_request_rules → item 0 → conditions → item 1
```
Attribute only allowed in commands_restrictions section
```
* Invalid condition 'sender-permission=write'. Attribute only allowed in commands_restrictions section @ queue_rules → item 0 → conditions → item 1
```
Attribute only allowed in commands_restrictions section
```
* Invalid condition 'sender=foobar'. Attribute only allowed in commands_restrictions section @ pull_request_rules → item 0 → conditions → item 0
```
Attribute only allowed in commands_restrictions section
```
* Invalid condition 'sender=foobar'. Attribute only allowed in commands_restrictions section @ queue_rules → item 0 → conditions → item 0
```
Attribute only allowed in commands_restrictions section
```
""".strip()
    )


@pytest.mark.parametrize(
    "condition,pull_request,expected_match,expected_related_checks",
    [
        pytest.param(
            conditions.RuleCondition.from_string("check-success=ci-1"),
            conftest.FakePullRequest(
                {
                    "check-success": ["ci-1", "ci-2"],
                    "check-failure": ["ci-3", "ci-4"],
                    "check": ["ci-1", "ci-2", "ci-3", "ci-4"],
                }
            ),
            True,
            ["ci-1"],
            id="if match is True",
        ),
        pytest.param(
            conditions.RuleCondition.from_string("check-success=ci-3"),
            conftest.FakePullRequest(
                {
                    "check-success": ["ci-1", "ci-2"],
                    "check-failure": ["ci-3", "ci-4"],
                    "check": ["ci-1", "ci-2", "ci-3", "ci-4"],
                }
            ),
            False,
            ["ci-3"],
            id="if match is False",
        ),
        pytest.param(
            conditions.RuleCondition.from_string("check-success=ci-3"),
            conftest.FakePullRequest(
                {
                    "check-success": [],  # type: ignore[dict-item]
                    "check-failure": [],  # type: ignore[dict-item]
                    "check": [],  # type: ignore[dict-item]
                }
            ),
            False,
            ["ci-3"],
            id="if checks not yet reported",
        ),
        pytest.param(
            conditions.RuleCondition.from_string("check-failure!=ci-3"),
            conftest.FakePullRequest(
                {
                    "check-success": ["ci-1", "ci-2"],
                    "check-failure": ["ci-3", "ci-4"],
                    "check": ["ci-1", "ci-2", "ci-3", "ci-4"],
                }
            ),
            False,
            ["ci-3"],
            id="if operator is not equal",
        ),
    ],
)
async def test_rule_condition_related_checks(
    condition: conditions.RuleCondition,
    pull_request: conftest.FakePullRequest,
    expected_match: bool,
    expected_related_checks: list[str],
) -> None:
    await condition(pull_request)

    assert condition.match is expected_match
    assert condition.related_checks == expected_related_checks


async def test_queue_rules_parameters() -> None:
    config = await utils.load_mergify_config(
        """
queue_rules:
   - name: foo
     conditions: []
     speculative_checks: 5
     batch_size: 6
     batch_max_wait_time: 60 s
     allow_inplace_checks: False
     disallow_checks_interruption_from_queues: [foo]
     checks_timeout: 60 m
     draft_bot_account: my-bot
     queue_branch_merge_method: fast-forward
     queue_branch_prefix: my-prefix
     allow_queue_branch_edit: True
     batch_max_failure_resolution_attempts: 10
"""
    )

    queue_rule = config["queue_rules"].rules[0]
    assert queue_rule.name == "foo"
    assert queue_rule.config["speculative_checks"] == 5
    assert queue_rule.config["batch_size"] == 6
    assert queue_rule.config["batch_max_wait_time"] == qr_config.PositiveInterval(
        "60 s"
    )
    assert queue_rule.config["allow_inplace_checks"] is False
    assert queue_rule.config["disallow_checks_interruption_from_queues"] == ["foo"]
    assert queue_rule.config["checks_timeout"] == qr_config.PositiveInterval("60 m")
    assert queue_rule.config["draft_bot_account"] == "my-bot"
    assert queue_rule.config["queue_branch_merge_method"] == "fast-forward"
    assert queue_rule.config["queue_branch_prefix"] == "my-prefix"
    assert queue_rule.config["allow_queue_branch_edit"] is True
    assert queue_rule.config["batch_max_failure_resolution_attempts"] == 10


def _dt(at: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(at).replace(tzinfo=datetime.UTC)


@pytest.mark.parametrize(
    "condition,pull_request,expected_match,expected_next_evaluation_at",
    [
        pytest.param(
            conditions.RuleCondition.from_string(
                "schedule=Mon-Fri 09:00-17:30[Europe/Paris]"
            ),
            conftest.FakePullRequest({"current-datetime": _dt("2022-01-10T12:00:00")}),
            True,
            _dt("2022-01-10T16:31:00"),
            id="in schedule",
        ),
        pytest.param(
            conditions.RuleCondition.from_string(
                "schedule=Mon-Fri 09:00-17:30[Europe/Paris]"
            ),
            conftest.FakePullRequest({"current-datetime": _dt("2022-01-10T00:00:00")}),
            False,
            _dt("2022-01-10T08:00:01"),
            id="out schedule",
        ),
        pytest.param(
            conditions.RuleCondition.from_string("base=main"),
            conftest.FakePullRequest(
                {"current-datetime": _dt("2022-01-10T00:00:00"), "base": "main"}
            ),
            True,
            date.DT_MAX,
            id="not a schedule",
        ),
    ],
)
async def test_rule_condition_next_evaluation_at(
    condition: conditions.RuleCondition,
    pull_request: conftest.FakePullRequest,
    expected_match: bool,
    expected_next_evaluation_at: datetime.datetime,
) -> None:
    await condition(pull_request)

    assert condition.match is expected_match
    assert condition.next_evaluation_at == expected_next_evaluation_at


def test_rule_condition_value() -> None:
    condition = conditions.RuleCondition.from_string(
        "schedule=Mon-Fri 09:00-17:30[Europe/Paris]"
    )
    expected_value = date.Schedule.from_string("Mon-Fri 09:00-17:30[Europe/Paris]")

    assert isinstance(condition.value, date.Schedule)
    assert condition.value == expected_value


@pytest.mark.parametrize(
    "condition,expected_operator",
    (
        (conditions.RuleCondition.from_string("base=main"), "="),
        (conditions.RuleCondition.from_string("base==main"), "="),
        (conditions.RuleCondition.from_string("base:main"), "="),
        (conditions.RuleCondition.from_string("base!=main"), "!="),
        (conditions.RuleCondition.from_string("base≠main"), "!="),
    ),
)
def test_rule_condition_operator(
    condition: conditions.RuleCondition, expected_operator: str
) -> None:
    assert condition.operator == expected_operator
