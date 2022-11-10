from base64 import encodebytes
from collections import abc
import copy
import dataclasses
import re
import typing
from unittest import mock

from freezegun import freeze_time
import pytest
import voluptuous

from mergify_engine import context
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine.clients import http
from mergify_engine.rules import InvalidRules
from mergify_engine.rules import conditions
from mergify_engine.tests import utils
from mergify_engine.tests.unit import conftest


def pull_request_rule_from_list(lst: typing.Any) -> rules.PullRequestRules:
    return typing.cast(
        rules.PullRequestRules,
        voluptuous.Schema(rules.get_pull_request_rules_schema())(lst),
    )


def test_valid_condition() -> None:
    c = conditions.RuleCondition("head~=bar")
    assert str(c) == "head~=bar"


def fake_expander(v: str) -> list[str]:
    return ["foo", "bar"]


async def test_expanders() -> None:
    rc = conditions.RuleCondition("author=@team")
    rc.partial_filter.value_expanders["author"] = fake_expander
    await rc(mock.Mock(author="foo"))
    assert rc.match

    copy_rc = rc.copy()
    await copy_rc(mock.Mock(author="foo"))
    assert copy_rc.match


def test_invalid_condition_re() -> None:
    with pytest.raises(voluptuous.Invalid):
        conditions.RuleCondition("head~=(bar")


@dataclasses.dataclass
class FakeQueuePullRequest(context.BasePullRequest):
    attrs: dict[str, context.ContextAttributeType]

    async def __getattr__(self, name: str) -> context.ContextAttributeType:
        fancy_name = name.replace("_", "-")
        try:
            return self.attrs[fancy_name]
        except KeyError:
            raise context.PullRequestAttributeError(name=fancy_name)


async def test_multiple_pulls_to_match() -> None:
    c = conditions.QueueRuleConditions(
        [
            conditions.RuleConditionCombination(
                {
                    "or": [
                        conditions.RuleCondition("base=main"),
                        conditions.RuleCondition("base=main"),
                    ]
                }
            )
        ]
    )
    assert await c([FakeQueuePullRequest({"number": 1, "base": "main"})])
    c = c.copy()
    assert await c([FakeQueuePullRequest({"number": 1, "base": "main"})])
    c = c.copy()
    assert not await c([FakeQueuePullRequest({"number": 1, "base": "other"})])
    c = c.copy()
    assert await c(
        [
            FakeQueuePullRequest({"number": 1, "base": "main"}),
            FakeQueuePullRequest({"number": 1, "base": "main"}),
        ]
    )
    c = c.copy()
    assert await c(
        [
            FakeQueuePullRequest({"number": 1, "base": "main"}),
            FakeQueuePullRequest({"number": 1, "base": "main"}),
            FakeQueuePullRequest({"number": 1, "base": "main"}),
        ]
    )
    c = c.copy()
    assert not await c(
        [
            FakeQueuePullRequest({"number": 1, "base": "main"}),
            FakeQueuePullRequest({"number": 1, "base": "main"}),
            FakeQueuePullRequest({"number": 1, "base": "other"}),
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
        {"name": "or", "conditions": [{"or": ["body:foo", "body:baz"]}], "actions": {}},
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
        (
            {
                "name": "not enough items or",
                "conditions": [{"or": ["label=foo"]}],
                "actions": {},
            },
            "length of value must be at least 2 for dictionary value @ data[0]['conditions'][0]['or']",
        ),
        (
            {
                "name": "not enough items and",
                "conditions": [{"and": []}],
                "actions": {},
            },
            "length of value must be at least 2 for dictionary value @ data[0]['conditions'][0]['and']",
        ),
    ),
)
def test_invalid_pull_request_rule(invalid: typing.Any, error: str) -> None:
    with pytest.raises(voluptuous.Invalid) as i:
        pull_request_rule_from_list([invalid])

    assert error in str(i.value)


async def test_same_pull_request_rules_name() -> None:
    with pytest.raises(rules.InvalidRules) as x:
        await rules.get_mergify_config_from_dict(
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
    with pytest.raises(rules.InvalidRules) as x:
        await rules.get_mergify_config_from_dict(
            mock.MagicMock(),
            {
                "queue_rules": [
                    {
                        "name": "default",
                        "conditions": ["schedule: MON-FRI 08:00-17:00"],
                        "allow_inplace_checks": False,
                    },
                    {
                        "name": "default",
                        "conditions": ["schedule: MON-FRI 08:00-17:00"],
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
    schema = await rules.get_mergify_config_from_file(mock.MagicMock(), config_file)
    assert isinstance(schema, dict)
    assert "pull_request_rules" in schema
    assert "queue_rules" in schema
    assert "defaults" in schema
    assert "commands_restrictions" in schema


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

    schema = await rules.get_mergify_config_from_file(mock.MagicMock(), config_file)
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

    schema = await rules.get_mergify_config_from_file(mock.MagicMock(), config_file)
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
                "/repos/Mergifyio/mergify-engine/contents/.mergify.yml", params={}
            ),
            mock.call(
                "/repos/Mergifyio/mergify-engine/contents/.mergify/config.yml",
                params={},
            ),
            mock.call(
                "/repos/Mergifyio/mergify-engine/contents/.github/mergify.yml",
                params={},
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
    with pytest.raises(InvalidRules):

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
        await rules.get_mergify_config_from_file(mock.MagicMock(), config_file)


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

    ir = rules.InvalidRules(i.value, ".mergify.yml")
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

    ir = rules.InvalidRules(i.value, ".mergify.yml")
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
    ir = rules.InvalidRules(i.value, ".mergify.yml")
    assert str(ir) == "expected a list for dictionary value @ pull_request_rules"

    with pytest.raises(voluptuous.Invalid) as i:
        rules.UserConfigurationSchema(rules.YamlSchema(""))
    assert str(i.value) == "expected a dictionary"
    ir = rules.InvalidRules(i.value, ".mergify.yml")
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
    ir = rules.InvalidRules(i.value, ".mergify.yml")
    assert (
        str(ir)
        == "expected str @ pull_request_rules → item 0 → actions → label → add → item 0"
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
    mergify_config = rules.MergifyConfig(
        {
            "extends": github_types.GitHubRepositoryName(".github"),
            "pull_request_rules": rules.PullRequestRules([]),
            "queue_rules": rules.QueueRules([]),
            "defaults": rules.Defaults({"actions": {}}),
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
    with pytest.raises(rules.InvalidRules) as i:
        await rules.get_mergify_config_from_dict(
            repository_ctxt, mergify_config["raw_config"], ""
        )
    assert (
        str(i.value)
        == "Only configuration from other repositories can be extended. @ extends"
    )


async def test_extends_limit(fake_repository: context.Repository) -> None:
    mergify_config = rules.MergifyConfig(
        {
            "extends": github_types.GitHubRepositoryName(".github"),
            "pull_request_rules": rules.PullRequestRules([]),
            "queue_rules": rules.QueueRules([]),
            "defaults": rules.Defaults({"actions": {}}),
            "commands_restrictions": {},
            "raw_config": {
                "extends": github_types.GitHubRepositoryName(".github"),
            },
        }
    )

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

        with pytest.raises(rules.InvalidRules) as i:
            await rules.get_mergify_config_from_dict(
                repository_ctxt, mergify_config["raw_config"], ""
            )
    assert (
        str(i.value)
        == "Maximum number of extended configuration reached. Limit is 1. @ extends"
    )


def test_user_binary_file() -> None:
    with pytest.raises(voluptuous.Invalid) as i:
        rules.UserConfigurationSchema(rules.YamlSchema(chr(4)))
    assert str(i.value) == "Invalid YAML at []"
    ir = rules.InvalidRules(i.value, ".mergify.yml")
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


async def test_get_pull_request_rule(
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
        elif url == "/repos/Mergifyio/mergify-engine/collaborators/jd/permission":
            return {"permission": "write"}
        elif url == "/repos/Mergifyio/mergify-engine/branches/main/protection":
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
    pull_request_rules = rules.PullRequestRules(
        [
            rules.PullRequestRule(
                name=rules.PullRequestRuleName("default"),
                disabled=None,
                conditions=conditions.PullRequestRuleConditions([]),
                actions={},
            )
        ]
    )

    match = await pull_request_rules.get_pull_request_rule(ctxt)
    assert [r.name for r in match.rules] == ["default"]
    assert [r.name for r in match.matching_rules] == ["default"]
    for rule in match.rules:
        assert rule.actions == {}

    pull_request_rules = pull_request_rule_from_list(
        [{"name": "hello", "conditions": ["base:main"], "actions": {}}]
    )

    match = await pull_request_rules.get_pull_request_rule(ctxt)
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

    match = await pull_request_rules.get_pull_request_rule(ctxt)
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

    match = await pull_request_rules.get_pull_request_rule(ctxt)
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

    match = await pull_request_rules.get_pull_request_rule(ctxt)
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

    match = await pull_request_rules.get_pull_request_rule(ctxt)
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

    match = await pull_request_rules.get_pull_request_rule(ctxt)
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
    match = await pull_request_rules.get_pull_request_rule(ctxt)

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

    match = await pull_request_rules.get_pull_request_rule(ctxt)
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

    match = await pull_request_rules.get_pull_request_rule(ctxt)
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

    match = await pull_request_rules.get_pull_request_rule(ctxt)
    assert [r.name for r in match.rules] == ["default"]
    assert [r.name for r in match.matching_rules] == ["default"]
    assert match.matching_rules[0].name == "default"
    assert match.matching_rules[0].conditions.match

    # Forbidden labels, when forbiden label set
    ctxt.pull["labels"] = [
        {"id": 0, "color": "#1234", "default": False, "name": "status/wip"}
    ]

    match = await pull_request_rules.get_pull_request_rule(ctxt)
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

    match = await pull_request_rules.get_pull_request_rule(ctxt)
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
    match = await pull_request_rules.get_pull_request_rule(ctxt)
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
    match = await pull_request_rules.get_pull_request_rule(ctxt)

    assert [r.name for r in match.rules] == ["default", "default"]
    assert list(match.matching_rules[0].actions.keys()) == ["merge"]
    assert len(match.matching_rules[0].conditions.condition.conditions) == 1
    assert not match.matching_rules[0].conditions.match
    group = match.matching_rules[0].conditions.condition.conditions[0]
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

    rules.merge_config_with_defaults(config, defaults)
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
    rules.merge_config_with_defaults(config, defaults)
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
    rules.merge_config_with_defaults(config, defaults)
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

    config = await rules.get_mergify_config_from_file(mock.MagicMock(), file)

    assert [list(rule.actions.keys()) for rule in config["pull_request_rules"]][0] == [
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

    with pytest.raises(rules.InvalidRules) as e:
        await rules.get_mergify_config_from_file(mock.MagicMock(), file)

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

    with pytest.raises(rules.InvalidRules) as e:
        await rules.get_mergify_config_from_file(mock.MagicMock(), file)

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

    assert await rules.get_mergify_config_from_file(mock.MagicMock(), file)
    config = await rules.get_mergify_config_from_file(mock.MagicMock(), file)

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

    with pytest.raises(rules.InvalidRules) as e:
        await rules.get_mergify_config_from_file(mock.MagicMock(), file)

    assert (
        str(e.value)
        == """* Invalid condition 'author=dependabot[bot] - status-success=Travis CI - Pull Request'. Invalid GitHub login @ pull_request_rules → item 0 → conditions → item 0
```
Invalid GitHub login
```
* expected a dictionary for dictionary value @ pull_request_rules → item 0 → actions
* extra keys not allowed @ pull_request_rules → item 0 → merge"""
    )


async def test_queue_rules_summary() -> None:
    schema = voluptuous.Schema(
        voluptuous.All(
            [voluptuous.Coerce(rules.RuleConditionSchema)],
            voluptuous.Coerce(conditions.QueueRuleConditions),
        )
    )

    c = schema(
        [
            "base=main",
            {"or": ["head=feature-1", "head=feature-2", "head=feature-3"]},
            {"or": ["label=urgent", "status-failure!=noway"]},
            {"or": ["label=bar", "check-success=first-ci"]},
            {"or": ["label=foo", "check-success!=first-ci"]},
            {"and": ["label=foo", "check-success=first-ci"]},
            {"and": ["label=foo", "check-success!=first-ci"]},
            {"not": {"and": ["label=fizz", "label=buzz"]}},
            "current-year=2018",
        ]
    )
    c.condition.conditions.extend(
        [
            conditions.RuleCondition(
                "#approved-reviews-by>=2",
                description="🛡 GitHub branch protection",
            ),
            conditions.RuleConditionCombination(
                {
                    "or": [
                        conditions.RuleCondition("check-success=my-awesome-ci"),
                        conditions.RuleCondition("check-neutral=my-awesome-ci"),
                        conditions.RuleCondition("check-skipped=my-awesome-ci"),
                    ]
                },
                description="🛡 GitHub branch protection",
            ),
            conditions.RuleCondition(
                "author=me",
                description="Another mechanism to get condtions",
            ),
        ]
    )

    pulls = [
        FakeQueuePullRequest(
            {
                "number": 1,
                "current-year": date.Year(2018),
                "author": "me",
                "base": "main",
                "head": "feature-1",
                "label": ["foo", "bar"],
                "check-success": ["first-ci", "my-awesome-ci"],
                "check-neutral": None,
                "check-skipped": None,
                "status-failure": ["noway"],
                "approved-reviews-by": ["jd", "sileht"],
            }
        ),
        FakeQueuePullRequest(
            {
                "number": 2,
                "current-year": date.Year(2018),
                "author": "me",
                "base": "main",
                "head": "feature-2",
                "label": ["foo", "urgent"],
                "check-success": ["first-ci", "my-awesome-ci"],
                "check-neutral": None,
                "check-skipped": None,
                "status-failure": ["noway"],
                "approved-reviews-by": ["jd", "sileht"],
            }
        ),
        FakeQueuePullRequest(
            {
                "number": 3,
                "current-year": date.Year(2018),
                "author": "not-me",
                "base": "main",
                "head": "feature-3",
                "label": ["foo", "urgent"],
                "check-success": ["first-ci", "my-awesome-ci"],
                "check-neutral": None,
                "check-skipped": None,
                "status-failure": ["noway"],
                "approved-reviews-by": ["jd", "sileht"],
            }
        ),
    ]
    await c(pulls)

    assert (
        c.get_summary()
        == """\
- `author=me` [Another mechanism to get condtions]
  - [X] #1
  - [X] #2
  - [ ] #3
- [ ] all of:
  - [ ] `check-success!=first-ci`
  - `label=foo`
    - [X] #1
    - [X] #2
    - [X] #3
- [ ] any of:
  - `label=urgent`
    - [ ] #1
    - [X] #2
    - [X] #3
  - [ ] `status-failure!=noway`
- `#approved-reviews-by>=2` [🛡 GitHub branch protection]
  - [X] #1
  - [X] #2
  - [X] #3
- [X] `base=main`
- [X] `current-year=2018`
- [X] all of:
  - [X] `check-success=first-ci`
  - `label=foo`
    - [X] #1
    - [X] #2
    - [X] #3
- [X] any of:
  - `head=feature-1`
    - [X] #1
    - [ ] #2
    - [ ] #3
  - `head=feature-2`
    - [ ] #1
    - [X] #2
    - [ ] #3
  - `head=feature-3`
    - [ ] #1
    - [ ] #2
    - [X] #3
- [X] any of:
  - [X] `check-success=first-ci`
  - `label=bar`
    - [X] #1
    - [ ] #2
    - [ ] #3
- [X] any of:
  - `label=foo`
    - [X] #1
    - [X] #2
    - [X] #3
  - [ ] `check-success!=first-ci`
- [X] any of [🛡 GitHub branch protection]:
  - [X] `check-success=my-awesome-ci`
  - [ ] `check-neutral=my-awesome-ci`
  - [ ] `check-skipped=my-awesome-ci`
- [X] not:
  - [ ] all of:
    - `label=buzz`
      - [ ] #1
      - [ ] #2
      - [ ] #3
    - `label=fizz`
      - [ ] #1
      - [ ] #2
      - [ ] #3
"""
    )


@freeze_time("2021-09-22T08:00:05", tz_offset=0)
async def test_rules_conditions_schedule() -> None:
    pulls = [
        FakeQueuePullRequest(
            {
                "number": 1,
                "author": "me",
                "base": "main",
                "current-timestamp": date.utcnow(),
                "current-time": date.utcnow(),
                "current-day": date.Day(22),
                "current-month": date.Month(9),
                "current-year": date.Year(2021),
                "current-day-of-week": date.DayOfWeek(3),
            }
        ),
    ]
    schema = voluptuous.Schema(
        voluptuous.All(
            [voluptuous.Coerce(rules.RuleConditionSchema)],
            voluptuous.Coerce(conditions.QueueRuleConditions),
        )
    )

    c = schema(
        [
            "base=main",
            "schedule=MON-FRI 08:00-17:00",
            "schedule=MONDAY-FRIDAY 10:00-12:00",
            "schedule=SAT-SUN 07:00-12:00",
        ]
    )

    await c(pulls)

    assert (
        c.get_summary()
        == """- [ ] `schedule=MONDAY-FRIDAY 10:00-12:00`
- [ ] `schedule=SAT-SUN 07:00-12:00`
- [X] `base=main`
- [X] `schedule=MON-FRI 08:00-17:00`
"""
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

    config = await rules.get_mergify_config_from_file(mock.MagicMock(), file)

    pull_request_rules = list(config["pull_request_rules"])
    assert pull_request_rules[0].actions["queue"].config["name"] == "default"
    assert pull_request_rules[0].actions["queue"].config["method"] == "squash"


def queue_rule_from_list(lst: typing.Any) -> rules.PullRequestRules:
    return typing.cast(
        rules.PullRequestRules,
        voluptuous.Schema(rules.get_pull_request_rules_schema())(lst),
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

    with pytest.raises(rules.InvalidRules) as i:
        await rules.get_mergify_config_from_file(mock.MagicMock(), file)
    assert (
        "disallow_checks_interruption_from_queues containes an unkown queue: whatever"
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

    config = await rules.get_mergify_config_from_file(mock.MagicMock(), file)
    assert config["queue_rules"][rules.QueueName("low")].config[
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

    with pytest.raises(rules.InvalidRules) as i:
        await rules.get_mergify_config_from_file(mock.MagicMock(), file)
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

    with pytest.raises(rules.InvalidRules) as i:
        await rules.get_mergify_config_from_file(mock.MagicMock(), file)
    assert (
        "* expected str for dictionary value @ queue_rules → item 0 → batch_max_wait_time"
        in str(i.value)
    )
    assert (
        "* not a valid value for dictionary value @ queue_rules → item 0 → checks_timeout"
        in str(i.value)
    )


async def test_condition_summary_simple() -> None:
    single_condition_checked = conditions.RuleCondition("base=main")
    single_condition_checked.match = True
    pr_conditions = conditions.PullRequestRuleConditions([single_condition_checked])

    expected_summary = "- [X] `base=main`\n"
    assert pr_conditions.get_summary() == expected_summary

    expected_summary = ""
    assert pr_conditions.get_unmatched_summary() == expected_summary


async def test_condition_summary_complex() -> None:
    schema = voluptuous.Schema(
        voluptuous.All(
            [voluptuous.Coerce(rules.RuleConditionSchema)],
            voluptuous.Coerce(conditions.PullRequestRuleConditions),
        )
    )
    pr_conditions = schema(
        [
            "base=main",
            {"or": ["label=foo", "label=bar"]},
            {"and": ["label=foo", "label=baz"]},
        ]
    )
    pr_conditions.condition.conditions[0].match = True
    pr_conditions.condition.conditions[2].conditions[1].match = True

    expected_summary = """- [ ] all of:
  - [ ] `label=foo`
  - [X] `label=baz`
- [ ] any of:
  - [ ] `label=bar`
  - [ ] `label=foo`
- [X] `base=main`
"""
    assert pr_conditions.get_summary() == expected_summary

    expected_summary = """- [ ] all of:
  - [ ] `label=foo`
- [ ] any of:
  - [ ] `label=bar`
  - [ ] `label=foo`
"""
    assert pr_conditions.get_unmatched_summary() == expected_summary


async def test_rule_condition_negation_summary() -> None:
    rule_condition_negation = rules.RuleConditionSchema(
        {"not": {"or": ["base=main", "label=foo"]}}
    )
    pr_conditions = conditions.PullRequestRuleConditions([rule_condition_negation])
    pr_conditions.condition.conditions[0].match = True

    expected_summary = """- [X] not:
  - [ ] any of:
    - [ ] `base=main`
    - [ ] `label=foo`
"""
    assert pr_conditions.get_summary() == expected_summary

    assert pr_conditions.get_unmatched_summary() == ""


async def test_has_unmatched_conditions() -> None:
    condition = conditions.RuleCondition("base=main")
    pr_conditions = conditions.PullRequestRuleConditions([condition])
    assert pr_conditions.has_unmatched_conditions()

    condition.match = True
    assert not pr_conditions.has_unmatched_conditions()


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

    assert (
        config["queue_rules"]  # type: ignore[union-attr]
        .rules[0]
        .conditions.condition.conditions[0]
        .condition
        == "status-success=continuous-integration/fake-ci"
    )
    assert (
        config["pull_request_rules"]  # type: ignore[union-attr]
        .rules[0]
        .conditions.condition.conditions[0]
        .condition
        == "base=main"
    )


async def test_rule_condition_negation_extract_raw_filter_tree() -> None:
    rule_condition_negation = rules.RuleConditionSchema(
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


async def test_render_big_nested_summary() -> None:
    c = conditions.QueueRuleConditions(
        [
            conditions.RuleConditionCombination(
                {
                    "or": [
                        conditions.RuleCondition("base=main"),
                        conditions.RuleConditionCombination(
                            {
                                "or": [
                                    conditions.RuleCondition("base=main"),
                                    conditions.RuleConditionCombination(
                                        {
                                            "or": [
                                                conditions.RuleCondition("base=main"),
                                                conditions.RuleConditionCombination(
                                                    {
                                                        "or": [
                                                            conditions.RuleCondition(
                                                                "base=main"
                                                            ),
                                                            conditions.RuleConditionCombination(
                                                                {
                                                                    "or": [
                                                                        conditions.RuleCondition(
                                                                            "base=main"
                                                                        ),
                                                                        conditions.RuleConditionCombination(
                                                                            {
                                                                                "or": [
                                                                                    conditions.RuleCondition(
                                                                                        "base=main"
                                                                                    ),
                                                                                ]
                                                                            }
                                                                        ),
                                                                    ]
                                                                }
                                                            ),
                                                        ]
                                                    }
                                                ),
                                            ]
                                        }
                                    ),
                                ]
                            }
                        ),
                    ]
                }
            )
        ]
    )

    summary = c.get_summary()
    summary_split = summary.strip().split("\n")
    assert summary_split[-1] == "            - [ ] `base=main`"


async def test_command_only_conditions(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    with pytest.raises(rules.InvalidRules) as e:
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
