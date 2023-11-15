from collections import abc
import typing
from unittest import mock

import pytest
import voluptuous

from mergify_engine import check_api
from mergify_engine import condition_value_querier
from mergify_engine import github_types
from mergify_engine import queue as merge_queue
from mergify_engine import rules
from mergify_engine import subscription
from mergify_engine.actions import queue
from mergify_engine.clients import http
from mergify_engine.rules import checks_status
from mergify_engine.rules.config import partition_rules as partr_config
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
        url: str,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> abc.AsyncGenerator[github_types.GitHubCheckRun, None]:
        if url == "/repos/Mergifyio/mergify-engine/commits/the-head-sha/status":
            return
        elif url == "/repos/Mergifyio/mergify-engine/commits/the-head-sha/check-runs":
            yield github_types.GitHubCheckRun(
                {
                    "head_sha": github_types.SHAType(
                        "ce587453ced02b1526dfb4cb910479d431683101",
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
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4",
                    ),
                    "after": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4",
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
                },
            )
            yield github_types.GitHubCheckRun(
                {
                    "head_sha": github_types.SHAType(
                        "ce587453ced02b1526dfb4cb910479d431683101",
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
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4",
                    ),
                    "after": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4",
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
                },
            )
            yield github_types.GitHubCheckRun(
                {
                    "head_sha": github_types.SHAType(
                        "ce587453ced02b1526dfb4cb910479d431683101",
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
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4",
                    ),
                    "after": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4",
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
                },
            )
            yield github_types.GitHubCheckRun(
                {
                    "head_sha": github_types.SHAType(
                        "ce587453ced02b1526dfb4cb910479d431683101",
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
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4",
                    ),
                    "after": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4",
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
                },
            )
        else:
            raise Exception(f"url not mocked: {url}")  # noqa: TRY002

    def item_call(
        url: str,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> github_types.GitHubBranch | github_types.GitHubContentFile:
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
                                "username": "",
                                "date": github_types.ISODateTimeType(""),
                            },
                            "author": {
                                "email": "",
                                "name": "",
                                "username": "",
                                "date": github_types.ISODateTimeType(""),
                            },
                        },
                        "committer": {
                            "type": "User",
                            "id": github_types.GitHubAccountIdType(1234),
                            "login": github_types.GitHubLogin("goo"),
                            "avatar_url": "https://example.com",
                        },
                        "author": {
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
                },
            )

        if url == "/repos/Mergifyio/mergify-engine/branches/main/protection":
            raise http.HTTPNotFound(
                message="boom",
                response=mock.Mock(),
                request=mock.Mock(),
            )

        if url == "/repos/Mergifyio/mergify-engine/contents/.mergify.yml":
            return github_types.GitHubContentFile(
                {
                    "content": "",
                    "type": "file",
                    "path": github_types.GitHubFilePath(".mergify.yml"),
                    "sha": github_types.SHAType(
                        "8ac2d8f970ab504e4d65351b10a2b5d8480bc38a",
                    ),
                    "encoding": "base64",
                },
            )

        raise Exception(f"url not mocked: {url}")  # noqa: TRY002

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
                    ],
                },
            ],
            check_api.Conclusion.PENDING,
        ),
        (
            [
                {
                    "or": [
                        {"and": ["title!=awesome", "check-success:pending"]},
                        {"and": ["title=foobar", "check-success:pending"]},
                    ],
                },
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
                        ],
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
            },
        ],
    )
    match = await rules.get_pull_request_rules_evaluator(ctxt)
    evaluated_rule = match.matching_rules[0]
    assert (
        await checks_status.get_rule_checks_status(
            ctxt.log,
            ctxt.repository,
            [condition_value_querier.PullRequest(ctxt)],
            evaluated_rule.conditions,
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
    ("queue_rule_config", "queue_executor_config"),
    (
        ({}, {}),
        (
            {"batch_size": 5, "queue_branch_merge_method": "fast-forward"},
            {},
        ),
        (
            {},
            {"update_method": "rebase", "update_bot_account": "test"},
        ),
    ),
)
async def test_required_status_checks_strict_compatibility_with_queue_rules(
    queue_rule_config: dict[str, typing.Any],
    queue_executor_config: dict[str, typing.Any],
) -> None:
    action = create_queue_action(queue_rule_config)
    ctxt = create_context_with_branch_protection_required_status_checks_strict()
    queue_executor = await queue.QueueExecutor.create(action, ctxt, mock.Mock())
    queue_executor.config.update(queue_executor_config)  # type: ignore[typeddict-item]

    # Nothing raised
    await queue.QueueExecutor._check_config_compatibility_with_branch_protection(
        ctxt,
        action.queue_rule.config,
        queue_executor.config,
    )


@pytest.mark.parametrize(
    (
        "queue_rule_config",
        "queue_executor_config",
        "expected_config_error_configuration",
        "expected_config_error_required_additional_configuration",
    ),
    (
        ({"batch_size": 2}, {"update_method": "merge"}, "batch_size>1", None),
        (
            {"speculative_checks": 2},
            {},
            "speculative_checks>1",
            None,
        ),
        (
            {"allow_inplace_checks": False},
            {},
            "allow_inplace_checks=false",
            None,
        ),
    ),
)
async def test_required_status_checks_strict_incompatibility_with_queue_rules(
    queue_rule_config: dict[str, typing.Any],
    queue_executor_config: dict[str, typing.Any],
    expected_config_error_configuration: str,
    expected_config_error_required_additional_configuration: str | None,
) -> None:
    action = create_queue_action(queue_rule_config)
    ctxt = create_context_with_branch_protection_required_status_checks_strict()
    queue_executor = await queue.QueueExecutor.create(action, ctxt, mock.Mock())
    queue_executor.config.update(queue_executor_config)  # type: ignore[typeddict-item]

    with pytest.raises(queue.IncompatibleBranchProtection) as e:
        await queue.QueueExecutor._check_config_compatibility_with_branch_protection(
            ctxt,
            action.queue_rule.config,
            queue_executor.config,
        )

    assert e.value.configuration == expected_config_error_configuration
    assert (
        e.value.required_additional_configuration
        == expected_config_error_required_additional_configuration
    )
    assert (
        e.value.branch_protection_setting
        == queue.BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT
    )

    if expected_config_error_required_additional_configuration is not None:
        assert e.value.message == (
            "The branch protection setting "
            f"`{queue.BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT}` is not compatible with `{expected_config_error_configuration}` "
            f"if `{expected_config_error_required_additional_configuration}` isn't set."
        )
    else:
        assert e.value.message == (
            "The branch protection setting "
            f"`{queue.BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT}` is not compatible with `{expected_config_error_configuration}` "
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
        },
    )

    action = queue.QueueAction({})
    action.queue_rule = queue_rules["queue_rules"]["default"]
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    evaluated_pull_request_rule = mock.Mock()
    executor = await action.executor_class.create(
        action,
        ctxt,
        evaluated_pull_request_rule,
    )
    executor._set_action_config_from_queue_rules()

    assert executor.config["commit_message_template"] == "test"
    assert executor.config["merge_method"] == "rebase"
    assert executor.config["merge_bot_account"] == "test"
    assert executor.config["update_method"] == "rebase"
    assert executor.config["update_bot_account"] == "test"


@pytest.mark.parametrize(
    (
        "update_method",
        "commit_message_template",
        "batch_size",
        "speculative_checks",
        "allow_inplace_checks",
        "expected_title",
        "expected_message",
    ),
    (
        (
            "merge",
            None,
            1,
            1,
            True,
            "`update_method: merge` is not compatible with fast-forward merge method",
            "`update_method` must be set to `rebase`.",
        ),
        (
            "rebase",
            "test",
            1,
            1,
            True,
            "Commit message can't be changed with fast-forward merge method",
            "`commit_message_template` must not be set if `merge_method: fast-forward` is set.",
        ),
        (
            "rebase",
            None,
            4,
            1,
            True,
            "batch_size > 1 is not compatible with fast-forward merge method",
            "The `merge_method` or the queue configuration must be updated.",
        ),
        (
            "rebase",
            None,
            1,
            4,
            True,
            "speculative_checks > 1 is not compatible with fast-forward merge method",
            "The `merge_method` or the queue configuration must be updated.",
        ),
        (
            "rebase",
            None,
            1,
            1,
            False,
            "allow_inplace_checks=False is not compatible with fast-forward merge method",
            "The `merge_method` or the queue configuration must be updated.",
        ),
    ),
)
async def test_check_method_fastforward_configuration(
    update_method: str,
    commit_message_template: str | None,
    batch_size: int,
    speculative_checks: int,
    allow_inplace_checks: bool,
    expected_title: str,
    expected_message: str,
    context_getter: conftest.ContextGetterFixture,
) -> None:
    queue_rules = rules.UserConfigurationSchema(
        {
            "queue_rules": [
                {
                    "name": "default",
                    "commit_message_template": commit_message_template,
                    "update_method": update_method,
                    "batch_size": batch_size,
                    "speculative_checks": speculative_checks,
                    "allow_inplace_checks": allow_inplace_checks,
                },
            ],
        },
    )

    action = queue.QueueAction({})
    action.queue_rule = queue_rules["queue_rules"]["default"]
    action.config["merge_method"] = "fast-forward"
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    evaluated_pull_request_rule = mock.Mock()
    executor = await action.executor_class.create(
        action,
        ctxt,
        evaluated_pull_request_rule,
    )
    executor._set_action_config_from_queue_rules()

    with pytest.raises(queue.InvalidQueueConfiguration) as e:
        executor._check_method_fastforward_configuration(
            config=executor.config,
            queue_rule=executor.queue_rule,
        )

    assert e.value.title == expected_title
    assert e.value.message == expected_message


async def test_check_queue_branch_merge_method_fastforward_with_partition_rules(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    config = rules.UserConfigurationSchema(
        {
            "queue_rules": [
                {
                    "name": "default",
                    "allow_inplace_checks": False,
                    "queue_branch_merge_method": "fast-forward",
                },
            ],
            "partition_rules": [
                {
                    "name": "projA",
                    "conditions": [],
                },
            ],
        },
    )

    action = queue.QueueAction({})
    action.partition_rules = config["partition_rules"]
    action.queue_rules = config["queue_rules"]
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    evaluated_pull_request_rule = mock.Mock()
    executor = await action.executor_class.create(
        action,
        ctxt,
        evaluated_pull_request_rule,
    )
    with pytest.raises(queue.InvalidQueueConfiguration) as e:
        await executor._set_action_queue_rule()

    assert e.value.title == "Invalid merge method with partition rules in use"
    assert (
        e.value.message
        == "Cannot use `fast-forward` merge method when using partition rules"
    )


@pytest.mark.parametrize(
    (
        "queue_rules",
        "expected_title",
    ),
    (
        (
            {
                "queue_rules": [
                    {
                        "name": "default",
                        "batch_size": 2,
                        "speculative_checks": 2,
                    },
                    {
                        "name": "test",
                        "batch_size": 2,
                        "speculative_checks": 2,
                    },
                ],
            },
            "Cannot use multiple queues.",
        ),
        (
            {
                "queue_rules": [
                    {
                        "name": "default",
                        "batch_size": 2,
                        "speculative_checks": 2,
                    },
                ],
            },
            "Cannot use `speculative_checks` with queue action.",
        ),
        (
            {
                "queue_rules": [
                    {
                        "name": "default",
                        "batch_size": 2,
                        "speculative_checks": 1,
                    },
                ],
            },
            "Cannot use `batch_size` with queue action.",
        ),
    ),
)
async def test_check_subscription_status(
    queue_rules: dict[str, typing.Any],
    expected_title: str,
    context_getter: conftest.ContextGetterFixture,
) -> None:
    queue_rules = rules.UserConfigurationSchema(queue_rules)

    action = queue.QueueAction({})
    action.queue_rule = queue_rules["queue_rules"]["default"]
    action.queue_rules = queue_rules["queue_rules"]
    action.config["priority"] = merge_queue.PriorityAliases.high.value
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.subscription.features = frozenset(
        {
            subscription.Features.PUBLIC_REPOSITORY,
            # Add this feature just to bypass the first test in the function
            # and make sure all the others still work
            subscription.Features.MERGE_QUEUE,
        },
    )

    evaluated_pull_request_rule = mock.Mock()
    executor = await action.executor_class.create(
        action,
        ctxt,
        evaluated_pull_request_rule,
    )
    executor.partition_rules = partr_config.PartitionRules([])
    executor._set_action_config_from_queue_rules()

    with pytest.raises(queue.InvalidQueueConfiguration) as e:
        await executor._check_subscription_status(
            config=executor.config,
            queue_rule=executor.queue_rule,
            queue_rules=executor.queue_rules,
            partition_rules=executor.partition_rules,
            ctxt=ctxt,
        )

    assert e.value.title == expected_title
    assert (
        e.value.message
        == "⚠ The [subscription](http://localhost:3000/github/Mergifyio/subscription) needs to be updated to enable this feature."
    )
