import json
from unittest import mock

import pytest
import respx

from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine.actions.backport import BackportAction
from mergify_engine.actions.rebase import RebaseAction
from mergify_engine.engine import commands_runner
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.tests.unit import conftest


async def get_empty_config() -> mergify_conf.MergifyConfig:
    return await mergify_conf.get_mergify_config_from_file(
        mock.MagicMock(),
        context.MergifyConfigFile(
            type="file",
            content="whatever",
            sha=github_types.SHAType("azertyuiop"),
            path=github_types.GitHubFilePath("whatever"),
            decoded_content="",
            encoding="base64",
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
    },
)


async def test_command_loader() -> None:
    with pytest.raises(commands_runner.CommandInvalidError):
        commands_runner.load_command(
            await get_empty_config(),
            "@mergifyio notexist foobar\n",
        )

    with pytest.raises(commands_runner.CommandInvalidError):
        commands_runner.load_command(
            await get_empty_config(),
            "@mergifyio comment foobar\n",
        )

    with pytest.raises(commands_runner.CommandInvalidError):
        commands_runner.load_command(
            await get_empty_config(),
            "@Mergifyio comment foobar\n",
        )

    with pytest.raises(commands_runner.NotACommandError):
        commands_runner.load_command(
            await get_empty_config(),
            "comment @Mergifyio test foobar\n",
        )

    for message in [
        "@mergify rebase",
        "@mergifyio rebase",
        "@Mergifyio rebase",
        "@mergifyio rebase\n",
        " @MergifyIO rebase",
        " @MergifyIO rebase\n",
        " @MergifyIO rebase  ",
        " @MergifyIO rebase  \n",
        "    @MergifyIO rebase",
        "@mergifyio rebase foobar",
        "@mergifyio rebase foobar\nsecondline\n",
    ]:
        command = commands_runner.load_command(await get_empty_config(), message)
        assert command.name == "rebase"
        assert isinstance(command.action, RebaseAction)

    command = commands_runner.load_command(
        await get_empty_config(),
        "@mergifyio backport branch-3.1 branch-3.2\nfoobar\n",
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
        encoding="base64",
    )
    config = await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)
    command = commands_runner.load_command(config, "@mergifyio backport")
    assert command.name == "backport"
    assert not command.command_args
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
    user: github_types.GitHubAccount,
    user_permission: str,
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


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
@pytest.mark.parametrize(
    ("user_id", "permission", "comment", "result"),
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
            commands_runner.UNKNOWN_COMMAND_MESSAGE,
        ),
        (
            123,
            "nothing",
            "@mergifyio something",
            commands_runner.UNKNOWN_COMMAND_MESSAGE,
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
            f"{settings.GITHUB_URL}/Mergifyio squash",
            "Pull request is already one-commit long",
        ),
        (
            666,
            "admin",
            "@mergifyio something",
            commands_runner.UNKNOWN_COMMAND_MESSAGE,
        ),
        (
            666,
            "write",
            "@mergifyio something",
            commands_runner.UNKNOWN_COMMAND_MESSAGE,
        ),
    ],
)
async def test_run_command_with_user(
    user_id: int | None,
    permission: str,
    comment: str,
    result: str | None,
    context_getter: conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
    fake_mergify_bot: github_types.GitHubAccount,
) -> None:
    if user_id is None:
        user_id = fake_mergify_bot["id"]
    user = create_fake_user(user_id)
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))

    if result and result != commands_runner.UNKNOWN_COMMAND_MESSAGE:
        respx_mock.get(
            f"{ctxt.base_url}/collaborators/{user['login']}/permission",
        ).respond(
            200,
            json={
                "permission": permission,
                "user": user,
            },
        )

    respx_mock.get(f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments").respond(
        200,
        json=[
            github_types.GitHubComment(
                {
                    "id": github_types.GitHubCommentIdType(1),
                    "url": "",
                    "created_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "updated_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "user": user,
                    "body": "unrelated",
                },
            ),
            github_types.GitHubComment(
                {
                    "id": github_types.GitHubCommentIdType(1),
                    "url": "",
                    "created_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "updated_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "user": user,
                    "body": comment,
                },
            ),
        ],
    )

    if result is not None:
        post_comment_router = respx_mock.post(
            f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments",
        ).respond(200, json={})

    await commands_runner.run_commands_tasks(
        ctxt=ctxt,
        mergify_config=await get_empty_config(),
    )

    if result is not None:
        assert post_comment_router.call_count == 1
        assert result in json.loads(post_comment_router.calls[0][0].content)["body"]


def test_extract_command_state_with_old_payload_format() -> None:
    user = create_fake_user()

    # bottom payload without attribute "action_is_running"
    body = """Waiting for conditions to match

<details>

- [ ] any of: [🛡 GitHub branch protection]
  - [ ] `check-neutral=continuous-integration/fake-ci`
  - [ ] `check-skipped=continuous-integration/fake-ci`
  - [ ] `check-success=continuous-integration/fake-ci`
- [X] `-draft` [📌 queue requirement]
- [X] `-mergify-configuration-changed` [📌 queue -> allow_merging_configuration_change setting requirement]
- [X] any of: [🔀 queue conditions]
  - [X] all of [📌 queue conditions of queue `default`]

</details>

<!---
DO NOT EDIT
-*- Mergify Payload -*-
{"command": "queue default", "conclusion": "neutral"}
-*- Mergify Payload End -*-
-->"""

    comment = github_types.GitHubComment(
        {
            "id": github_types.GitHubCommentIdType(1),
            "url": "",
            "created_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
            "updated_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
            "user": user,
            "body": body,
        },
    )

    command_state = commands_runner.extract_command_state(
        comment=comment,
        mergify_bot=user,
        pendings=mock.Mock(),
        finished_commands=mock.Mock(),
    )

    assert command_state.action_is_running is False


async def test_run_command_with_wrong_arg(
    context_getter: conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
) -> None:
    user = create_fake_user()

    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    respx_mock.get(f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments").respond(
        200,
        json=[
            github_types.GitHubComment(
                {
                    "id": github_types.GitHubCommentIdType(1),
                    "url": "",
                    "created_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "updated_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "user": user,
                    "body": "@mergifyio squash invalid-arg",
                },
            ),
        ],
    )

    post_comment_router = respx_mock.post(
        f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments",
    ).respond(200, json={})

    await commands_runner.run_commands_tasks(
        ctxt=ctxt,
        mergify_config=await get_empty_config(),
    )

    assert post_comment_router.call_count == 1
    assert (
        json.loads(post_comment_router.calls[0][0].content)["body"]
        == """> squash invalid-arg

#### ❌ Sorry but I didn't understand the arguments of the command `squash`. Please consult [the commands documentation](https://docs.mergify.com/commands/) 📚.



<!---
DO NOT EDIT
-*- Mergify Payload -*-
{"command": "squash invalid-arg", "conclusion": "failure", "action_is_running": false}
-*- Mergify Payload End -*-
-->"""
    )


@pytest.mark.parametrize(
    "command_name",
    (
        "backport",
        "copy",
        "squash",
        "update",
        "rebase",
        "refresh",
        "queue",
        "unqueue",
        "requeue",
    ),
)
async def test_run_command_with_no_subscription(
    context_getter: conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
    command_name: str,
) -> None:
    user = create_fake_user()
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    respx_mock.get(f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments").respond(
        200,
        json=[
            github_types.GitHubComment(
                {
                    "id": github_types.GitHubCommentIdType(1),
                    "url": "",
                    "created_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "updated_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "user": user,
                    "body": f"@mergifyio {command_name}",
                },
            ),
        ],
    )

    post_comment_router = respx_mock.post(
        f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments",
    ).respond(200, json={})

    await commands_runner.run_commands_tasks(
        ctxt=ctxt,
        mergify_config=await get_empty_config(),
    )

    assert post_comment_router.call_count == 1
    assert (
        json.loads(post_comment_router.calls[0][0].content)["body"]
        == f"""> {command_name}

#### ⚠️ Cannot use the command `{command_name}`

<details>

⚠ The [subscription](http://localhost:3000/github/Mergifyio/subscription) needs to be updated to enable this feature.

</details>


<!---
DO NOT EDIT
-*- Mergify Payload -*-
{{"command": "{command_name}", "conclusion": "action_required", "action_is_running": false}}
-*- Mergify Payload End -*-
-->"""
    )


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
@pytest.mark.parametrize(
    ("command_restriction", "user_permission", "is_command_allowed"),
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
        # Default
        (None, "read", False),
    ],
)
async def test_commands_restrictions_sender_permission(
    command_restriction: str | None,
    user_permission: str,
    is_command_allowed: bool,
    context_getter: conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
) -> None:
    config = (
        ""
        if command_restriction is None
        else f"""
commands_restrictions:
  squash:
    conditions:
    - {command_restriction}
"""
    )

    mergify_config = await mergify_conf.get_mergify_config_from_file(
        mock.MagicMock(),
        context.MergifyConfigFile(
            type="file",
            content="whatever",
            sha=github_types.SHAType("azertyuiop"),
            path=github_types.GitHubFilePath("whatever"),
            decoded_content=config,
            encoding="base64",
        ),
    )

    user = create_fake_user()

    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository._caches.mergify_config.set(mergify_config)

    respx_mock.get(f"{ctxt.base_url}/collaborators/{user['login']}/permission").respond(
        200,
        json={
            "permission": user_permission,
            "user": user,
        },
    )
    respx_mock.get(f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments").respond(
        200,
        json=[
            github_types.GitHubComment(
                {
                    "id": github_types.GitHubCommentIdType(1),
                    "url": "",
                    "created_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "updated_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "user": user,
                    "body": "@mergifyio squash",
                },
            ),
        ],
    )
    post_comment_router = respx_mock.post(
        f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments",
    ).respond(200, json={})

    await commands_runner.run_commands_tasks(ctxt=ctxt, mergify_config=mergify_config)

    if is_command_allowed:
        expected_result_str = "Pull request is already one-commit long"
    else:
        expected_result_str = "Command disallowed due to [command restrictions]"

    assert post_comment_router.call_count == 1
    assert (
        expected_result_str
        in json.loads(post_comment_router.calls[0][0].content)["body"]
    )


async def test_pending_commands_ordering(
    context_getter: conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
    fake_mergify_bot: github_types.GitHubAccount,
) -> None:
    user = create_fake_user()
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))

    respx_mock.get(f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments").respond(
        200,
        json=[
            github_types.GitHubComment(
                {
                    "id": github_types.GitHubCommentIdType(1),
                    "url": "",
                    "created_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "updated_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "user": user,
                    "body": "@mergifyio squash",
                },
            ),
            github_types.GitHubComment(
                {
                    "id": github_types.GitHubCommentIdType(1),
                    "url": "",
                    "created_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "updated_at": github_types.ISODateTimeType("2003-02-15T00:00:00Z"),
                    "user": user,
                    "body": "@mergifyio rebase",
                },
            ),
            github_types.GitHubComment(
                {
                    "id": github_types.GitHubCommentIdType(2),
                    "url": "",
                    "created_at": github_types.ISODateTimeType("2003-02-15T01:00:00Z"),
                    "updated_at": github_types.ISODateTimeType("2003-02-15T01:00:00Z"),
                    "user": fake_mergify_bot,
                    "body": commands_runner.prepare_message(
                        "squash",
                        check_api.Result(
                            check_api.Conclusion.PENDING,
                            "pending",
                            "pending",
                        ),
                        True,
                    ),
                },
            ),
        ],
    )
    pendings = await commands_runner.get_pending_commands_to_run_from_comments(ctxt)
    states = list(pendings.values())
    assert len(states) == 2
    assert states[0].command == "rebase"
    assert states[0].github_comment_result is None
    assert states[1].command == "squash"
    assert states[1].github_comment_result is not None
    assert states[1].github_comment_result["id"] == 2
