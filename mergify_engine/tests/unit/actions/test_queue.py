from collections import abc
import typing
from unittest import mock

import pytest
import voluptuous

from mergify_engine import check_api
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine.actions import queue
from mergify_engine.clients import http
from mergify_engine.rules import checks_status
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.tests.unit import conftest


def pull_request_rule_from_list(lst: typing.Any) -> prr_config.PullRequestRules:
    return typing.cast(
        prr_config.PullRequestRules,
        voluptuous.Schema(prr_config.get_pull_request_rules_schema())(lst),
    )


@pytest.fixture
def fake_client() -> mock.Mock:
    async def items_call(
        url: str, *args: typing.Any, **kwargs: typing.Any
    ) -> abc.AsyncGenerator[github_types.GitHubCheckRun, None]:
        if url == "/repos/Mergifyio/mergify-engine/commits/the-head-sha/status":
            return
        elif url == "/repos/Mergifyio/mergify-engine/commits/the-head-sha/check-runs":
            yield github_types.GitHubCheckRun(
                {
                    "head_sha": github_types.SHAType(
                        "ce587453ced02b1526dfb4cb910479d431683101"
                    ),
                    "details_url": "https://example.com",
                    "status": "completed",
                    "conclusion": "failure",
                    "name": "failure",
                    "id": 1234,
                    "app": {
                        "id": 1234,
                        "name": "CI",
                        "slug": "ci",
                        "owner": {
                            "type": "User",
                            "id": github_types.GitHubAccountIdType(1234),
                            "login": github_types.GitHubLogin("goo"),
                            "avatar_url": "https://example.com",
                        },
                    },
                    "external_id": "",
                    "pull_requests": [],
                    "before": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "after": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "started_at": github_types.ISODateTimeType(""),
                    "completed_at": None,
                    "html_url": "https://example.com",
                    "check_suite": {"id": 1234},
                    "output": {
                        "summary": "",
                        "title": "It runs!",
                        "text": "",
                        "annotations": [],
                        "annotations_count": 0,
                        "annotations_url": "https://example.com",
                    },
                }
            )
            yield github_types.GitHubCheckRun(
                {
                    "head_sha": github_types.SHAType(
                        "ce587453ced02b1526dfb4cb910479d431683101"
                    ),
                    "details_url": "https://example.com",
                    "status": "completed",
                    "conclusion": "success",
                    "name": "success",
                    "id": 1235,
                    "app": {
                        "id": 1234,
                        "name": "CI",
                        "slug": "ci",
                        "owner": {
                            "type": "User",
                            "id": github_types.GitHubAccountIdType(1234),
                            "login": github_types.GitHubLogin("goo"),
                            "avatar_url": "https://example.com",
                        },
                    },
                    "external_id": "",
                    "pull_requests": [],
                    "before": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "after": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "started_at": github_types.ISODateTimeType(""),
                    "completed_at": None,
                    "html_url": "https://example.com",
                    "check_suite": {"id": 1234},
                    "output": {
                        "summary": "",
                        "title": "It runs!",
                        "text": "",
                        "annotations": [],
                        "annotations_count": 0,
                        "annotations_url": "https://example.com",
                    },
                }
            )
            yield github_types.GitHubCheckRun(
                {
                    "head_sha": github_types.SHAType(
                        "ce587453ced02b1526dfb4cb910479d431683101"
                    ),
                    "details_url": "https://example.com",
                    "status": "completed",
                    "conclusion": "neutral",
                    "name": "neutral",
                    "id": 1236,
                    "app": {
                        "id": 1234,
                        "name": "CI",
                        "slug": "ci",
                        "owner": {
                            "type": "User",
                            "id": github_types.GitHubAccountIdType(1234),
                            "login": github_types.GitHubLogin("goo"),
                            "avatar_url": "https://example.com",
                        },
                    },
                    "external_id": "",
                    "pull_requests": [],
                    "before": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "after": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "started_at": github_types.ISODateTimeType(""),
                    "completed_at": None,
                    "html_url": "https://example.com",
                    "check_suite": {"id": 1234},
                    "output": {
                        "summary": "",
                        "title": "It runs!",
                        "text": "",
                        "annotations": [],
                        "annotations_count": 0,
                        "annotations_url": "https://example.com",
                    },
                }
            )
            yield github_types.GitHubCheckRun(
                {
                    "head_sha": github_types.SHAType(
                        "ce587453ced02b1526dfb4cb910479d431683101"
                    ),
                    "details_url": "https://example.com",
                    "status": "in_progress",
                    "conclusion": None,
                    "name": "pending",
                    "id": 1237,
                    "app": {
                        "id": 1234,
                        "name": "CI",
                        "slug": "ci",
                        "owner": {
                            "type": "User",
                            "id": github_types.GitHubAccountIdType(1234),
                            "login": github_types.GitHubLogin("goo"),
                            "avatar_url": "https://example.com",
                        },
                    },
                    "external_id": "",
                    "pull_requests": [],
                    "before": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "after": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "started_at": github_types.ISODateTimeType(""),
                    "completed_at": None,
                    "html_url": "https://example.com",
                    "check_suite": {"id": 1234},
                    "output": {
                        "summary": "",
                        "title": "It runs!",
                        "text": "",
                        "annotations": [],
                        "annotations_count": 0,
                        "annotations_url": "https://example.com",
                    },
                }
            )
        else:
            raise Exception(f"url not mocked: {url}")

    def item_call(
        url: str, *args: typing.Any, **kwargs: typing.Any
    ) -> github_types.GitHubBranch:
        if url == "/repos/Mergifyio/mergify-engine/branches/main":
            return github_types.GitHubBranch(
                {
                    "commit": {
                        "sha": github_types.SHAType("sha1"),
                        "parents": [],
                        "commit": {
                            "message": "",
                            "verification": {"verified": False},
                            "committer": {
                                "email": "",
                                "name": "",
                                "date": github_types.ISODateTimeType(""),
                            },
                            "author": {
                                "email": "",
                                "name": "",
                                "date": github_types.ISODateTimeType(""),
                            },
                        },
                        "committer": {
                            "type": "User",
                            "id": github_types.GitHubAccountIdType(1234),
                            "login": github_types.GitHubLogin("goo"),
                            "avatar_url": "https://example.com",
                        },
                    },
                    "protection": {
                        "enabled": False,
                        "required_status_checks": {"contexts": [], "strict": False},
                    },
                    "protected": False,
                    "name": github_types.GitHubRefType("foobar"),
                }
            )
        if url == "/repos/Mergifyio/mergify-engine/branches/main/protection":
            raise http.HTTPNotFound(
                message="boom", response=mock.Mock(), request=mock.Mock()
            )
        else:
            raise Exception(f"url not mocked: {url}")

    client = mock.Mock()
    client.item = mock.AsyncMock(side_effect=item_call)
    client.items = items_call
    return client


@pytest.mark.parametrize(
    "conditions,conclusion",
    (
        (
            ["title=awesome", "check-neutral:neutral", "check-success:success"],
            check_api.Conclusion.SUCCESS,
        ),
        (
            ["title!=awesome", "check-neutral:neutral", "check-success:success"],
            check_api.Conclusion.FAILURE,
        ),
        (
            ["title=awesome", "check-neutral:neutral", "check-success:pending"],
            check_api.Conclusion.PENDING,
        ),
        (
            ["title=awesome", "check-neutral:pending", "check-success:pending"],
            check_api.Conclusion.PENDING,
        ),
        (
            ["title=awesome", "check-neutral:notexists", "check-success:success"],
            check_api.Conclusion.PENDING,
        ),
        (
            ["title=awesome", "check-neutral:failure", "check-success:success"],
            check_api.Conclusion.FAILURE,
        ),
        (
            [
                {
                    "or": [
                        {"and": ["title=awesome", "check-success:pending"]},
                        {"and": ["title!=awesome", "check-success:pending"]},
                    ]
                }
            ],
            check_api.Conclusion.PENDING,
        ),
        (
            [
                {
                    "or": [
                        {"and": ["title!=awesome", "check-success:pending"]},
                        {"and": ["title=foobar", "check-success:pending"]},
                    ]
                }
            ],
            check_api.Conclusion.FAILURE,
        ),
        (
            [
                {
                    "not": {
                        "and": [
                            "title!=awesome",
                            "check-neutral:neutral",
                            "check-success:success",
                        ]
                    },
                },
            ],
            check_api.Conclusion.SUCCESS,
        ),
    ),
)
async def test_get_rule_checks_status(
    conditions: typing.Any,
    conclusion: check_api.Conclusion,
    context_getter: conftest.ContextGetterFixture,
    fake_client: mock.Mock,
) -> None:
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = fake_client
    rules = pull_request_rule_from_list(
        [
            {
                "name": "hello",
                "conditions": conditions,
                "actions": {},
            }
        ]
    )
    match = await rules.get_pull_request_rules_evaluator(ctxt)
    evaluated_rule = match.matching_rules[0]
    assert (
        await checks_status.get_rule_checks_status(
            ctxt.log, ctxt.repository, [ctxt.pull_request], evaluated_rule
        )
    ) == conclusion


def create_queue_action(queue_rule_config: dict[str, typing.Any]) -> queue.QueueAction:
    action = queue.QueueAction({})

    default_queue_rule_config = {
        "name": "default",
        "speculative_checks": 1,
        "allow_inplace_checks": True,
        "batch_size": 1,
        "queue_branch_merge_method": None,
    }
    default_queue_rule_config.update(queue_rule_config)

    action.queue_rule = mock.Mock()
    action.queue_rule.config = default_queue_rule_config

    return action


def create_context_with_branch_protection_required_status_checks_strict() -> (
    mock.AsyncMock
):
    protection = {
        "required_status_checks": {
            "strict": True,
            "contexts": [],
        },
        "required_pull_request_reviews": None,
        "restrictions": None,
        "enforce_admins": False,
    }
    ctxt = mock.AsyncMock()
    ctxt.repository.get_branch_protection.return_value = protection
    return ctxt


@pytest.mark.parametrize(
    ("queue_rule_config"),
    (
        pytest.param({}),
        pytest.param({"batch_size": 5, "queue_branch_merge_method": "fast-forward"}),
    ),
)
async def test_required_status_checks_strict_compatibility_with_queue_rules(
    queue_rule_config: dict[str, typing.Any]
) -> None:
    action = create_queue_action(queue_rule_config)
    ctxt = create_context_with_branch_protection_required_status_checks_strict()

    # Nothing raised
    await queue.QueueExecutor._check_config_compatibility_with_branch_protection(
        action.queue_rule, ctxt
    )


@pytest.mark.parametrize(
    ("queue_rule_config", "expected_config_error"),
    (
        ({"batch_size": 2}, "batch_size>1"),
        ({"speculative_checks": 2}, "speculative_checks>1"),
        ({"allow_inplace_checks": False}, "allow_inplace_checks=false"),
    ),
)
async def test_required_status_checks_strict_incompatibility_with_queue_rules(
    queue_rule_config: dict[str, typing.Any], expected_config_error: str
) -> None:
    action = create_queue_action(queue_rule_config)
    ctxt = create_context_with_branch_protection_required_status_checks_strict()

    with pytest.raises(queue.IncompatibleBranchProtection) as e:
        await queue.QueueExecutor._check_config_compatibility_with_branch_protection(
            action.queue_rule, ctxt
        )

    assert e.value.configuration == expected_config_error
    assert (
        e.value.branch_protection_setting
        == queue.BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT
    )
    assert (
        e.value.message == "The branch protection setting "
        f"`{queue.BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT}` is not compatible with `{expected_config_error}` "
        "and must be unset."
    )


async def test_action_rules_in_queue_rules(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    queue_rules = rules.UserConfigurationSchema(
        {
            "queue_rules": [
                {
                    "name": "default",
                    "commit_message_template": "test",
                    "merge_method": "rebase",
                    "merge_bot_account": "test",
                    "update_method": "rebase",
                    "update_bot_account": "test",
                    "merge_conditions": [],
                },
            ],
        }
    )

    action = queue.QueueAction({})
    action.queue_rule = queue_rules["queue_rules"]["default"]
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    evaluated_pull_request_rule = mock.Mock()
    executor = await action.executor_class.create(
        action, ctxt, evaluated_pull_request_rule
    )
    executor._set_action_config_from_queue_rules()

    assert executor.config["commit_message_template"] == "test"
    assert executor.config["method"] == "rebase"
    assert executor.config["merge_bot_account"] == "test"
    assert executor.config["update_method"] == "rebase"
    assert executor.config["update_bot_account"] == "test"
