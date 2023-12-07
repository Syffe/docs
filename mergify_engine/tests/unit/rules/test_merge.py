import typing
from unittest import mock

import pytest

from mergify_engine.rules.config import mergify as mergify_conf


source_config = {
    "pull_request_rules": [
        {
            "actions": {
                "queue": {
                    "commit_message_template": "{{ "
                    "title "
                    "}} "
                    "(#{{ "
                    "number "
                    "}})\n"
                    "\n"
                    "{{ "
                    "body "
                    "}}\n",
                    "method": "squash",
                    "name": "hotfix",
                },
            },
            "conditions": [
                "base=main",
                "author=@devs",
                "check-success=semantic-pull-request",
                "label=hotfix",
                "title~=^(revert|fix)",
                "#approved-reviews-by>=1",
                "#changes-requested-reviews-by=0",
                "label!=work in progress",
                "label!=manual merge",
            ],
            "name": "automatic merge for hotfix",
        },
        {
            "actions": {
                "queue": {
                    "commit_message_template": "{{ "
                    "title "
                    "}} "
                    "(#{{ "
                    "number "
                    "}})\n"
                    "\n"
                    "{{ "
                    "body "
                    "}}\n",
                    "method": "squash",
                    "name": "default",
                },
            },
            "conditions": [
                "base=main",
                "check-success=semantic-pull-request",
                {
                    "and": [
                        {"or": ["-label=docker", "check-success=docker"]},
                        "check-success=import-checks",
                        "check-success=pep8",
                        "check-success=test",
                        "check-success=docs",
                    ],
                },
                "check-success=Rule: feature requirements (post_check)",
                "check-success=Rule: testing requirements (post_check)",
                "#approved-reviews-by>=2",
                "#changes-requested-reviews-by=0",
                "#review-threads-unresolved=0",
                "label!=work in progress",
                "label!=manual merge",
            ],
            "name": "automatic merge",
        },
        {
            "actions": {
                "queue": {
                    "commit_message_template": "{{ "
                    "title "
                    "}} "
                    "(#{{ "
                    "number "
                    "}})\n"
                    "\n"
                    "{{ "
                    "body "
                    "}}\n",
                    "method": "rebase",
                    "name": "lowprio",
                },
            },
            "conditions": [
                {
                    "and": [
                        {"or": ["-label=docker", "check-success=docker"]},
                        "check-success=import-checks",
                        "check-success=pep8",
                        "check-success=test",
                        "check-success=docs",
                    ],
                },
                "check-success=semantic-pull-request",
                "author=mergify-ci-bot",
                "label!=work in progress",
                "label!=manual merge",
                "title~=^chore: bump",
                "#commits=1",
                "head~=^clifus/",
            ],
            "name": "automatic merge for clifus version bump",
        },
        {
            "actions": {
                "queue": {
                    "commit_message_template": None,
                    "method": "rebase",
                    "name": "lowprio",
                },
            },
            "conditions": [
                {
                    "and": [
                        {"or": ["-label=docker", "check-success=docker"]},
                        "check-success=import-checks",
                        "check-success=pep8",
                        "check-success=test",
                        "check-success=docs",
                    ],
                },
                "check-success=semantic-pull-request",
                "author=dependabot[bot]",
                "label!=work in progress",
                "label!=manual merge",
                "#commits=1",
            ],
            "name": "automatic merge from dependabot",
        },
        {
            "actions": {"comment": {"message": "@dependabot recreate"}},
            "conditions": ["author=dependabot[bot]", "conflict"],
            "name": "dependabot conflict fixer",
        },
        {
            "actions": {"dismiss_reviews": {}},
            "conditions": [],
            "name": "dismiss reviews",
        },
        {
            "actions": {"request_reviews": {"teams": ["devs"]}},
            "conditions": [
                "-author=dependabot[bot]",
                "-author=mergify-ci-bot",
                "label!=work in progress",
                "-merged",
                "-closed",
                {
                    "and": [
                        {"or": ["-label=docker", "check-success=docker"]},
                        "check-success=import-checks",
                        "check-success=pep8",
                        "check-success=test",
                        "check-success=docs",
                    ],
                },
                "check-success=Rule: feature requirements (post_check)",
                "check-success=Rule: testing requirements (post_check)",
                "#approved-reviews-by=0",
                "#changes-requested-reviews-by=0",
            ],
            "name": "request review",
        },
        {
            "actions": {
                "comment": {
                    "message": "@{{author}} this "
                    "pull request is "
                    "now in conflict "
                    "😩",
                },
                "label": {"add": ["conflict"]},
            },
            "conditions": ["conflict", "-closed"],
            "name": "warn on conflicts",
        },
        {
            "actions": {"label": {"remove": ["conflict"]}},
            "conditions": ["-conflict"],
            "name": "remove conflict label if not needed",
        },
        {
            "actions": {"label": {"add": ["Review Threads Unresolved"]}},
            "conditions": ["#review-threads-unresolved>0"],
            "name": "label on unresolved",
        },
        {
            "actions": {"label": {"remove": ["Review Threads Unresolved"]}},
            "conditions": ["#review-threads-unresolved=0"],
            "name": "unlabel on resolved",
        },
        {
            "actions": {
                "comment": {"message": "Your hotfix is failing CI @{{author}} 🥺"},
            },
            "conditions": ["label=hotfix", "#check-failure>0"],
            "name": "warn on CI failure for hotfix",
        },
        {
            "actions": {
                "post_check": {
                    "title": "{% if "
                    "check_succeed "
                    "%}\n"
                    "Feature "
                    "requirements are "
                    "present.\n"
                    "{% else %}\n"
                    "Feature "
                    "requirements are "
                    "missing.\n"
                    "{% endif %}\n",
                },
            },
            "conditions": [
                {
                    "or": [
                        "-title~=^feat",
                        {
                            "and": [
                                {
                                    "or": [
                                        "label=Skip release note",
                                        "files~=^releasenotes/notes",
                                    ],
                                },
                                {
                                    "or": [
                                        "label=Skip documentation",
                                        "files~=^docs/source",
                                    ],
                                },
                                "body~=MRGFY-",
                            ],
                        },
                    ],
                },
            ],
            "name": "feature requirements",
        },
        {
            "actions": {
                "post_check": {
                    "title": "{% if "
                    "check_succeed "
                    "%}\n"
                    "Testing "
                    "requirements are "
                    "present.\n"
                    "{% else %}\n"
                    "Testing "
                    "requirements are "
                    "missing.\n"
                    "{% endif %}\n",
                },
            },
            "conditions": [
                {
                    "or": [
                        "label=skip tests",
                        "-title~=^(feat|fix)",
                        "files~=mergify_engine/tests",
                    ],
                },
            ],
            "name": "testing requirements",
        },
    ],
    "queue_rules": [
        {
            "allow_inplace_checks": True,
            "conditions": [
                {
                    "and": [
                        {"or": ["-label=docker", "check-success=docker"]},
                        "check-success=import-checks",
                        "check-success=pep8",
                        "check-success=test",
                        "check-success=docs",
                    ],
                },
            ],
            "name": "hotfix",
            "speculative_checks": 5,
        },
        {
            "allow_inplace_checks": True,
            "conditions": [
                {
                    "and": [
                        {"or": ["-label=docker", "check-success=docker"]},
                        "check-success=import-checks",
                        "check-success=pep8",
                        "check-success=test",
                        "check-success=docs",
                    ],
                },
                "schedule=Mon-Fri 09:00-17:30[Europe/Paris]",
            ],
            "name": "default",
            "speculative_checks": 5,
        },
        {
            "allow_inplace_checks": True,
            "batch_max_wait_time": "5min",
            "batch_size": 5,
            "conditions": [
                {
                    "and": [
                        {"or": ["-label=docker", "check-success=docker"]},
                        "check-success=import-checks",
                        "check-success=pep8",
                        "check-success=test",
                        "check-success=docs",
                    ],
                },
                "schedule=Mon-Fri 09:30-17:00[Europe/Paris]",
            ],
            "name": "lowprio",
            "speculative_checks": 3,
        },
    ],
}


# NOTE(Syffe): The tests contained in this test file are using
# the keyword "conditions" instead of "merge_conditions" for queue_rules config
# in order to keep the backward compatibility tested. They should not be changed
# until we totally deprecate the "conditions" keyword.
def test_merge_raw_configs() -> None:
    dest_config = {
        "commands_restrictions": {"copy": {"conditions": ["sender=toto"]}},
        "extends": ".github",
        "pull_request_rules": [
            {
                "actions": {"merge": {}},
                "conditions": ["base=override_permission"],
                "name": "automatic merge",
            },
        ],
        "queue_rules": [
            {
                "allow_inplace_checks": False,
                "conditions": ["schedule: MON-FRI 06:06-06:06"],
                "name": "hotfix",
            },
        ],
    }

    expected_result = {
        "commands_restrictions": {"copy": {"conditions": ["sender=toto"]}},
        "extends": ".github",
        "pull_request_rules": [
            {
                "actions": {"merge": {}},
                "conditions": ["base=override_permission"],
                "name": "automatic merge",
            },
            {
                "actions": {
                    "queue": {
                        "commit_message_template": "{{ "
                        "title "
                        "}} "
                        "(#{{ "
                        "number "
                        "}})\n"
                        "\n"
                        "{{ "
                        "body "
                        "}}\n",
                        "method": "squash",
                        "name": "hotfix",
                    },
                },
                "conditions": [
                    "base=main",
                    "author=@devs",
                    "check-success=semantic-pull-request",
                    "label=hotfix",
                    "title~=^(revert|fix)",
                    "#approved-reviews-by>=1",
                    "#changes-requested-reviews-by=0",
                    "label!=work in progress",
                    "label!=manual merge",
                ],
                "name": "automatic merge for hotfix",
            },
            {
                "actions": {
                    "queue": {
                        "commit_message_template": "{{ "
                        "title "
                        "}} "
                        "(#{{ "
                        "number "
                        "}})\n"
                        "\n"
                        "{{ "
                        "body "
                        "}}\n",
                        "method": "rebase",
                        "name": "lowprio",
                    },
                },
                "conditions": [
                    {
                        "and": [
                            {"or": ["-label=docker", "check-success=docker"]},
                            "check-success=import-checks",
                            "check-success=pep8",
                            "check-success=test",
                            "check-success=docs",
                        ],
                    },
                    "check-success=semantic-pull-request",
                    "author=mergify-ci-bot",
                    "label!=work in progress",
                    "label!=manual merge",
                    "title~=^chore: bump",
                    "#commits=1",
                    "head~=^clifus/",
                ],
                "name": "automatic merge for clifus version bump",
            },
            {
                "actions": {
                    "queue": {
                        "commit_message_template": None,
                        "method": "rebase",
                        "name": "lowprio",
                    },
                },
                "conditions": [
                    {
                        "and": [
                            {"or": ["-label=docker", "check-success=docker"]},
                            "check-success=import-checks",
                            "check-success=pep8",
                            "check-success=test",
                            "check-success=docs",
                        ],
                    },
                    "check-success=semantic-pull-request",
                    "author=dependabot[bot]",
                    "label!=work in progress",
                    "label!=manual merge",
                    "#commits=1",
                ],
                "name": "automatic merge from dependabot",
            },
            {
                "actions": {"comment": {"message": "@dependabot recreate"}},
                "conditions": ["author=dependabot[bot]", "conflict"],
                "name": "dependabot conflict fixer",
            },
            {
                "actions": {"dismiss_reviews": {}},
                "conditions": [],
                "name": "dismiss reviews",
            },
            {
                "actions": {"request_reviews": {"teams": ["devs"]}},
                "conditions": [
                    "-author=dependabot[bot]",
                    "-author=mergify-ci-bot",
                    "label!=work in progress",
                    "-merged",
                    "-closed",
                    {
                        "and": [
                            {"or": ["-label=docker", "check-success=docker"]},
                            "check-success=import-checks",
                            "check-success=pep8",
                            "check-success=test",
                            "check-success=docs",
                        ],
                    },
                    "check-success=Rule: feature requirements (post_check)",
                    "check-success=Rule: testing requirements (post_check)",
                    "#approved-reviews-by=0",
                    "#changes-requested-reviews-by=0",
                ],
                "name": "request review",
            },
            {
                "actions": {
                    "comment": {
                        "message": "@{{author}} this "
                        "pull request is "
                        "now in conflict "
                        "😩",
                    },
                    "label": {"add": ["conflict"]},
                },
                "conditions": ["conflict", "-closed"],
                "name": "warn on conflicts",
            },
            {
                "actions": {"label": {"remove": ["conflict"]}},
                "conditions": ["-conflict"],
                "name": "remove conflict label if not needed",
            },
            {
                "actions": {"label": {"add": ["Review Threads Unresolved"]}},
                "conditions": ["#review-threads-unresolved>0"],
                "name": "label on unresolved",
            },
            {
                "actions": {"label": {"remove": ["Review Threads Unresolved"]}},
                "conditions": ["#review-threads-unresolved=0"],
                "name": "unlabel on resolved",
            },
            {
                "actions": {
                    "comment": {"message": "Your hotfix is failing CI @{{author}} 🥺"},
                },
                "conditions": ["label=hotfix", "#check-failure>0"],
                "name": "warn on CI failure for hotfix",
            },
            {
                "actions": {
                    "post_check": {
                        "title": "{% if "
                        "check_succeed "
                        "%}\n"
                        "Feature "
                        "requirements are "
                        "present.\n"
                        "{% else %}\n"
                        "Feature "
                        "requirements are "
                        "missing.\n"
                        "{% endif %}\n",
                    },
                },
                "conditions": [
                    {
                        "or": [
                            "-title~=^feat",
                            {
                                "and": [
                                    {
                                        "or": [
                                            "label=Skip release note",
                                            "files~=^releasenotes/notes",
                                        ],
                                    },
                                    {
                                        "or": [
                                            "label=Skip documentation",
                                            "files~=^docs/source",
                                        ],
                                    },
                                    "body~=MRGFY-",
                                ],
                            },
                        ],
                    },
                ],
                "name": "feature requirements",
            },
            {
                "actions": {
                    "post_check": {
                        "title": "{% if "
                        "check_succeed "
                        "%}\n"
                        "Testing "
                        "requirements are "
                        "present.\n"
                        "{% else %}\n"
                        "Testing "
                        "requirements are "
                        "missing.\n"
                        "{% endif %}\n",
                    },
                },
                "conditions": [
                    {
                        "or": [
                            "label=skip tests",
                            "-title~=^(feat|fix)",
                            "files~=mergify_engine/tests",
                        ],
                    },
                ],
                "name": "testing requirements",
            },
        ],
        "queue_rules": [
            {
                "allow_inplace_checks": False,
                "conditions": ["schedule: MON-FRI 06:06-06:06"],
                "name": "hotfix",
            },
            {
                "allow_inplace_checks": True,
                "conditions": [
                    {
                        "and": [
                            {"or": ["-label=docker", "check-success=docker"]},
                            "check-success=import-checks",
                            "check-success=pep8",
                            "check-success=test",
                            "check-success=docs",
                        ],
                    },
                    "schedule=Mon-Fri 09:00-17:30[Europe/Paris]",
                ],
                "name": "default",
                "speculative_checks": 5,
            },
            {
                "allow_inplace_checks": True,
                "batch_max_wait_time": "5min",
                "batch_size": 5,
                "conditions": [
                    {
                        "and": [
                            {"or": ["-label=docker", "check-success=docker"]},
                            "check-success=import-checks",
                            "check-success=pep8",
                            "check-success=test",
                            "check-success=docs",
                        ],
                    },
                    "schedule=Mon-Fri 09:30-17:00[Europe/Paris]",
                ],
                "name": "lowprio",
                "speculative_checks": 3,
            },
        ],
    }

    mergify_conf.merge_raw_configs(source_config, dest_config)
    assert dest_config == expected_result


def test_merge_raw_configs_empty() -> None:
    config: dict[str, typing.Any] = {}
    mergify_conf.merge_raw_configs({}, config)
    assert config == {}


def test_merge_raw_configs_src_empty() -> None:
    config = {
        "queue_rules": [
            {
                "allow_inplace_checks": False,
                "conditions": ["schedule: MON-FRI 06:06-06:06"],
                "name": "hotfix",
            },
        ],
    }
    mergify_conf.merge_raw_configs({}, config)
    assert config == {
        "queue_rules": [
            {
                "allow_inplace_checks": False,
                "conditions": ["schedule: MON-FRI 06:06-06:06"],
                "name": "hotfix",
            },
        ],
    }


def test_merge_raw_configs_dest_empty() -> None:
    config: dict[str, typing.Any] = {}
    mergify_conf.merge_raw_configs(
        {
            "queue_rules": [
                {
                    "allow_inplace_checks": False,
                    "conditions": ["schedule: MON-FRI 06:06-06:06"],
                    "name": "hotfix",
                },
            ],
        },
        config,
    )
    assert config == {
        "queue_rules": [
            {
                "allow_inplace_checks": False,
                "conditions": ["schedule: MON-FRI 06:06-06:06"],
                "name": "hotfix",
            },
        ],
    }


def test_merge_raw_override_and_new_rules() -> None:
    config = {
        "queue_rules": [
            {
                "allow_inplace_checks": False,
                "conditions": ["schedule: MON-FRI 00:00-00:00"],
                "name": "old_rule",
            },
            {
                "allow_inplace_checks": False,
                "conditions": ["schedule: MON-FRI 06:06-06:06"],
                "name": "new_rule",
            },
        ],
    }
    mergify_conf.merge_raw_configs(
        {
            "queue_rules": [
                {
                    "allow_inplace_checks": True,
                    "conditions": ["schedule: MON-FRI 06:06-06:06"],
                    "name": "new_rule",
                },
                {
                    "allow_inplace_checks": False,
                    "conditions": ["schedule: MON-FRI 00:00-00:00"],
                    "name": "hotfix",
                },
            ],
        },
        config,
    )
    assert config == {
        "queue_rules": [
            {
                "allow_inplace_checks": False,
                "conditions": ["schedule: MON-FRI 00:00-00:00"],
                "name": "old_rule",
            },
            {
                "allow_inplace_checks": False,
                "conditions": ["schedule: MON-FRI 06:06-06:06"],
                "name": "new_rule",
            },
            {
                "allow_inplace_checks": False,
                "conditions": ["schedule: MON-FRI 00:00-00:00"],
                "name": "hotfix",
            },
        ],
    }


@pytest.mark.parametrize(
    ("extended_defaults", "dest_defaults", "expected_result"),
    [
        (
            {"actions": {"copy": {"labels": ["copied"], "bot_account": "Autobot"}}},
            {"actions": {"copy": {"bot_account": "my_super_bot"}}},
            {
                "actions": {
                    "copy": {"labels": ["copied"], "bot_account": "my_super_bot"},
                },
            },
        ),
        (
            {},
            {"actions": {"copy": {"bot_account": "my_super_bot"}}},
            {"actions": {"copy": {"bot_account": "my_super_bot"}}},
        ),
        (
            {"actions": {"copy": {"bot_account": "my_super_bot"}}},
            {},
            {"actions": {"copy": {"bot_account": "my_super_bot"}}},
        ),
        (
            {
                "actions": {
                    "copy": {"labels": ["copied"], "bot_account": "Autobot"},
                    "merge": {"labels": ["merged"]},
                },
            },
            {
                "actions": {
                    "copy": {"bot_account": "my_super_bot"},
                    "backport": {"branches": ["crashme"]},
                },
            },
            {
                "actions": {
                    "copy": {"labels": ["copied"], "bot_account": "my_super_bot"},
                    "backport": {"branches": ["crashme"]},
                    "merge": {"labels": ["merged"]},
                },
            },
        ),
        (
            {},
            {},
            {},
        ),
    ],
)
def test_merge_defaults(
    extended_defaults: mergify_conf.Defaults,
    dest_defaults: mergify_conf.Defaults,
    expected_result: mergify_conf.Defaults,
) -> None:
    mergify_conf.merge_defaults(extended_defaults, dest_defaults)
    assert dest_defaults == expected_result


async def test_merge_rules_and_defaults() -> None:
    config = {
        "extends": "extended_configuration.yml",
        "defaults": {"actions": {"copy": {"bot_account": "my_super_bot"}}},
        "pull_request_rules": [
            {
                "name": "new_rule",
                "conditions": ["label=comment"],
                "actions": {"copy": {"branches": ["dev"]}},
            },
        ],
    }

    config_to_extend = {
        "defaults": {
            "actions": {"copy": {"labels": ["copied"], "bot_account": "Autobot"}},
        },
        "pull_request_rules": [
            {
                "name": "extended_rule",
                "conditions": ["label=comment"],
                "actions": {"copy": {"branches": ["dev"]}},
            },
        ],
    }

    expected_result = {
        "extends": "extended_configuration.yml",
        "pull_request_rules": [
            {
                "name": "new_rule",
                "conditions": ["label=comment"],
                "actions": {
                    "copy": {
                        "branches": ["dev"],
                        "labels": ["copied"],
                        "bot_account": "my_super_bot",
                    },
                },
            },
            {
                "name": "extended_rule",
                "conditions": ["label=comment"],
                "actions": {
                    "copy": {
                        "branches": ["dev"],
                        "labels": ["copied"],
                        "bot_account": "my_super_bot",
                    },
                },
            },
        ],
    }

    defaults = config_to_extend.pop("defaults")
    mocked_ctxt = mock.MagicMock()
    with mock.patch(
        "mergify_engine.rules.config.mergify.get_mergify_extended_config",
        return_value={
            "defaults": defaults,
            "raw_config": config_to_extend,
        },
    ):
        merged_config = await mergify_conf.get_mergify_config_from_dict(
            mocked_ctxt,
            config,
            "",
            allow_extend=True,
        )
    assert merged_config["raw_config"] == expected_result
