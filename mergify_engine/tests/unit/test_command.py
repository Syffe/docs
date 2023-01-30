from unittest import mock

import pytest

from mergify_engine import config
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine.actions.backport import BackportAction
from mergify_engine.actions.rebase import RebaseAction
from mergify_engine.engine import commands_runner
from mergify_engine.tests.unit import conftest


async def get_empty_config() -> rules.MergifyConfig:
    return await rules.get_mergify_config_from_file(
        mock.MagicMock(),
        context.MergifyConfigFile(
            type="file",
            content="whatever",
            sha=github_types.SHAType("azertyuiop"),
            path=github_types.GitHubFilePath("whatever"),
            decoded_content="",
        ),
    )


FAKE_COMMENT = github_types.GitHubComment(
    {
        "id": github_types.GitHubCommentIdType(123),
        "url": "",
        "body": "",
        "user": {
            "id": github_types.GitHubAccountIdType(123),
            "login": github_types.GitHubLogin("foobar"),
            "type": "User",
            "avatar_url": "",
        },
        "created_at": github_types.ISODateTimeType("2021-06-01T18:41:39Z"),
        "updated_at": github_types.ISODateTimeType("2021-06-01T18:41:39Z"),
    }
)


async def test_command_loader() -> None:
    with pytest.raises(commands_runner.CommandInvalid):
        commands_runner.load_command(
            await get_empty_config(), "@mergifyio notexist foobar\n"
        )

    with pytest.raises(commands_runner.CommandInvalid):
        commands_runner.load_command(
            await get_empty_config(), "@mergifyio comment foobar\n"
        )

    with pytest.raises(commands_runner.CommandInvalid):
        commands_runner.load_command(
            await get_empty_config(), "@Mergifyio comment foobar\n"
        )

    with pytest.raises(commands_runner.NotACommand):
        commands_runner.load_command(
            await get_empty_config(), "comment @Mergifyio test foobar\n"
        )

    for message in [
        "@mergify rebase",
        "@mergifyio rebase",
        "@Mergifyio rebase",
        "@mergifyio rebase\n",
        "@mergifyio rebase foobar",
        "@mergifyio rebase foobar\nsecondline\n",
    ]:
        command = commands_runner.load_command(await get_empty_config(), message)
        assert command.name == "rebase"
        assert isinstance(command.action, RebaseAction)

    command = commands_runner.load_command(
        await get_empty_config(), "@mergifyio backport branch-3.1 branch-3.2\nfoobar\n"
    )
    assert command.name == "backport"
    assert command.command_args == "branch-3.1 branch-3.2"
    assert isinstance(command.action, BackportAction)
    assert command.action.config == {
        "branches": ["branch-3.1", "branch-3.2"],
        "bot_account": None,
        "regexes": [],
        "ignore_conflicts": True,
        "labels": [],
        "label_conflicts": "conflicts",
        "assignees": [],
        "title": "{{ title }} (backport #{{ number }})",
        "body": "This is an automatic backport of pull request #{{number}} done by [Mergify](https://mergify.com).\n{{ cherry_pick_error }}\n\n---\n\n"
        + constants.MERGIFY_PULL_REQUEST_DOC,
    }


async def test_command_loader_with_defaults() -> None:
    raw_config = """
defaults:
  actions:
    backport:
      branches:
        - branch-3.1
        - branch-3.2
      ignore_conflicts: false
"""

    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content=raw_config,
    )
    config = await rules.get_mergify_config_from_file(mock.MagicMock(), file)
    command = commands_runner.load_command(config, "@mergifyio backport")
    assert command.name == "backport"
    assert command.command_args == ""
    assert isinstance(command.action, BackportAction)
    assert command.action.config == {
        "assignees": [],
        "branches": ["branch-3.1", "branch-3.2"],
        "bot_account": None,
        "regexes": [],
        "ignore_conflicts": False,
        "labels": [],
        "label_conflicts": "conflicts",
        "title": "{{ title }} (backport #{{ number }})",
        "body": "This is an automatic backport of pull request #{{number}} done by [Mergify](https://mergify.com).\n{{ cherry_pick_error }}\n\n---\n\n"
        + constants.MERGIFY_PULL_REQUEST_DOC,
    }


def create_fake_user(user_id: int = 1) -> github_types.GitHubAccount:
    return github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(user_id),
            "login": github_types.GitHubLogin("wall-e"),
            "type": "Bot",
            "avatar_url": "https://avatars.githubusercontent.com/u/583231?v=4",
        },
    )


def create_fake_installation_client(
    user: github_types.GitHubAccount, user_permission: str
) -> mock.Mock:
    client = mock.Mock()
    client.item = mock.AsyncMock()
    client.item.return_value = {
        "permission": user_permission,
        "user": user,
    }
    client.post = mock.AsyncMock()
    client.post.return_value = mock.Mock()
    return client


@pytest.mark.parametrize(
    "user_id,permission,comment,result",
    [
        (
            666,
            "nothing",
            "not a command",
            None,
        ),
        (
            None,
            "nothing",
            "@mergifyio something",
            "Sorry but I didn't understand the command",
        ),
        (
            123,
            "nothing",
            "@mergifyio something",
            "Sorry but I didn't understand the command",
        ),
        (
            666,
            "admin",
            "@mergifyio squash",
            "Pull request is already one-commit long",
        ),
        (
            666,
            "admin",
            f"{config.GITHUB_URL}/Mergifyio squash",
            "Pull request is already one-commit long",
        ),
        (
            666,
            "admin",
            "@mergifyio something",
            "Sorry but I didn't understand the command",
        ),
        (
            666,
            "write",
            "@mergifyio something",
            "Sorry but I didn't understand the command",
        ),
        (
            666,
            "write",
            "@mergifyio something",
            "Sorry but I didn't understand the command",
        ),
    ],
)
async def test_run_command_with_user(
    user_id: int | None,
    permission: str,
    comment: str,
    result: str | None,
    context_getter: conftest.ContextGetterFixture,
    fake_mergify_bot: github_types.GitHubAccount,
) -> None:
    if user_id is None:
        user_id = fake_mergify_bot["id"]
    user = create_fake_user(user_id)
    client = create_fake_installation_client(user, permission)
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client
    ctxt._get_consolidated_data = mock.AsyncMock()  # type: ignore[assignment]

    await commands_runner.handle(
        ctxt=ctxt,
        mergify_config=await get_empty_config(),
        comment_command="unrelated",
        user=user,
    )
    assert len(client.post.call_args_list) == 0

    await commands_runner.handle(
        ctxt=ctxt,
        mergify_config=await get_empty_config(),
        comment_command=comment,
        user=user,
    )

    if result is None:
        assert len(client.post.call_args_list) == 0
    else:
        assert len(client.post.call_args_list) == 1
        assert result in client.post.call_args_list[0][1]["json"]["body"]


async def test_run_command_with_wrong_arg(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    user = create_fake_user()
    client = mock.Mock()
    client.post = mock.AsyncMock()
    client.post.return_value = mock.Mock()

    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client

    await commands_runner.handle(
        ctxt=ctxt,
        mergify_config=await get_empty_config(),
        comment_command="@mergifyio squash invalid-arg",
        user=user,
    )

    assert len(client.post.call_args_list) == 1
    assert (
        client.post.call_args_list[0][1]["json"]["body"]
        == """> squash invalid-arg

#### ❌ Sorry but I didn't understand the arguments of the command `squash`. Please consult [the commands documentation](https://docs.mergify.com/commands/) 📚.



<!---
DO NOT EDIT
-*- Mergify Payload -*-
{"command": "squash invalid-arg", "conclusion": "failure"}
-*- Mergify Payload End -*-
-->"""
    )


@pytest.mark.parametrize(
    "command_restriction,user_permission,is_command_allowed",
    [
        ("sender-permission=admin", "admin", True),
        ("sender-permission=admin", "write", False),
        ("sender-permission=write", "admin", False),
        ("sender-permission=write", "write", True),
        ("sender-permission!=write", "admin", True),
        ("sender-permission!=write", "write", False),
        ("sender-permission<admin", "admin", False),
        ("sender-permission<admin", "write", True),
        ("sender-permission<=admin", "admin", True),
        ("sender-permission<=admin", "write", True),
        ("sender-permission>write", "write", False),
        ("sender-permission>write", "admin", True),
        ("sender-permission>=write", "write", True),
        ("sender-permission>=write", "admin", True),
    ],
)
async def test_commands_restrictions_sender_permission(
    command_restriction: str,
    user_permission: str,
    is_command_allowed: bool,
    context_getter: conftest.ContextGetterFixture,
) -> None:
    mergify_config = await rules.get_mergify_config_from_file(
        mock.MagicMock(),
        context.MergifyConfigFile(
            type="file",
            content="whatever",
            sha=github_types.SHAType("azertyuiop"),
            path=github_types.GitHubFilePath("whatever"),
            decoded_content=f"""
commands_restrictions:
  squash:
    conditions:
    - {command_restriction}
""",
        ),
    )

    user = create_fake_user()
    client = create_fake_installation_client(user, user_permission)
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client

    await commands_runner.handle(
        ctxt=ctxt,
        mergify_config=mergify_config,
        comment_command="@mergify squash",
        user=user,
    )

    if is_command_allowed:
        expected_result_str = "Pull request is already one-commit long"
    else:
        expected_result_str = "Command disallowed due to [command restrictions]"

    client.post.assert_called_once()
    assert expected_result_str in client.post.call_args_list[0][1]["json"]["body"]


async def test_default_commands_restrictions(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    mergify_config_file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content="",
    )

    mergify_config = await rules.get_mergify_config_from_file(
        mock.MagicMock(), mergify_config_file
    )

    user = create_fake_user()
    client = create_fake_installation_client(user, "read")
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client
    ctxt._get_consolidated_data = mock.AsyncMock()  # type: ignore[assignment]

    await commands_runner.handle(
        ctxt=ctxt,
        mergify_config=mergify_config,
        comment_command="@mergify squash",
        user=user,
    )

    client.post.assert_called_once()

    expected_result_str = "Command disallowed due to [command restrictions]"
    assert expected_result_str in client.post.call_args_list[0][1]["json"]["body"]
