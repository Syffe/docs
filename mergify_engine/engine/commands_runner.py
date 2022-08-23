# -*- encoding: utf-8 -*-
#
# Copyright © 2018–2021 Mergify SAS
#
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import collections
import dataclasses
import re
import typing

import daiquiri
from datadog import statsd
import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import config
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.rules import conditions


LOG = daiquiri.getLogger(__name__)

COMMAND_MATCHER = re.compile(
    rf"^(?:{config.GITHUB_URL.rstrip('/')}/|@)Mergify(?:|io) (\w*)(.*)",  # noqa: BLK100
    re.IGNORECASE,
)

MERGE_QUEUE_COMMAND_MESSAGE = "Command not allowed on merge queue pull request."
UNKNOWN_COMMAND_MESSAGE = "Sorry but I didn't understand the command. Please consult [the commands documentation](https://docs.mergify.com/commands.html) \U0001F4DA."
INVALID_COMMAND_ARGS_MESSAGE = "Sorry but I didn't understand the arguments of the command `{command}`. Please consult [the commands documentation](https://docs.mergify.com/commands/) \U0001F4DA."  # noqa
CONFIGURATION_CHANGE_MESSAGE = (
    "Sorry but this action cannot run when the configuration is updated"
)


class Command(typing.NamedTuple):
    name: str
    args: str
    action: actions.Action


@dataclasses.dataclass
class CommandInvalid(Exception):
    message: str


@dataclasses.dataclass
class NotACommand(Exception):
    message: str


def prepare_message(command_full: str, result: check_api.Result) -> str:
    # NOTE: Do not serialize this with Mergify JSON encoder:
    # we don't want to allow loading/unloading weird value/classes
    # this could be modified by a user, so we keep it really straightforward
    payload = {
        "command": command_full,
        "conclusion": result.conclusion.value,
    }
    details = ""
    if result.summary:
        details = f"<details>\n\n{result.summary}\n\n</details>\n"

    message = f"""> {command_full}

#### {result.conclusion.emoji} {result.title}

{details}

"""

    message += utils.get_mergify_payload(payload)
    return message


def load_command(
    mergify_config: rules.MergifyConfig,
    message: str,
) -> Command:
    """Load an action from a message."""
    action_classes = actions.get_commands()
    match = COMMAND_MATCHER.search(message)

    if not match:
        raise NotACommand(
            "Comment contains '@Mergify/io' tag but is not aimed to be executed as a command"
        )

    if match[1] in action_classes:
        action_name = match[1]
        action_class = action_classes[action_name]
        command_args = match[2].strip()

        action_config = {}
        if defaults_actions := mergify_config["defaults"].get("actions"):
            if default_action_config := defaults_actions.get(action_name):
                action_config = default_action_config

        action_config.update(action_class.command_to_config(command_args))
        try:
            action = action_class(action_config)
            action.validate_config(mergify_config)
        except voluptuous.Invalid:
            raise CommandInvalid(
                INVALID_COMMAND_ARGS_MESSAGE.format(command=action_name)
            )
        return Command(action_name, command_args, action)

    raise CommandInvalid(UNKNOWN_COMMAND_MESSAGE)


async def on_each_event(event: github_types.GitHubEventIssueComment) -> None:
    action_classes = actions.get_commands()
    match = COMMAND_MATCHER.search(event["comment"]["body"])
    if match and match[1] in action_classes:
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_name = event["repository"]["name"]
        installation_json = await github.get_installation_from_account_id(owner_id)
        async with github.aget_client(installation_json) as client:
            await client.post(
                f"/repos/{owner_login}/{repo_name}/issues/comments/{event['comment']['id']}/reactions",
                json={"content": "+1"},
                api_version="squirrel-girl",
            )


class LastUpdatedOrderedDict(collections.OrderedDict[str, github_types.GitHubComment]):
    def __setitem__(self, key: str, value: github_types.GitHubComment) -> None:
        super().__setitem__(key, value)
        self.move_to_end(key)


async def run_pending_commands_tasks(
    ctxt: context.Context, mergify_config: rules.MergifyConfig
) -> None:
    if ctxt.is_merge_queue_pr():
        # We don't allow any command yet
        return

    pendings = LastUpdatedOrderedDict()
    async for comment in typing.cast(
        typing.AsyncIterator[github_types.GitHubComment],
        ctxt.client.items(
            f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments",
            resource_name="comments",
            page_limit=20,
        ),
    ):

        if comment["user"]["id"] != config.BOT_USER_ID:
            continue

        payload = utils.get_hidden_payload_from_comment_body(comment["body"])
        if not payload:
            continue

        if (
            not isinstance(payload, dict)
            or "command" not in payload
            or "conclusion" not in payload
        ):
            LOG.warning("got command with invalid payload", payload=payload)
            continue

        command = payload["command"]
        conclusion_str = payload["conclusion"]

        try:
            conclusion = check_api.Conclusion(conclusion_str)
        except ValueError:
            LOG.error("Unable to load conclusions %s", conclusion_str)
            continue

        if conclusion == check_api.Conclusion.PENDING:
            pendings[command] = comment
        elif command in pendings:
            del pendings[command]

    for pending, comment in pendings.items():
        await handle(
            ctxt,
            mergify_config,
            f"@Mergifyio {pending}",
            None,
            comment_result=comment,
        )


async def run_command(
    ctxt: context.Context,
    mergify_config: rules.MergifyConfig,
    command: Command,
    user: typing.Optional[github_types.GitHubAccount],
) -> typing.Tuple[check_api.Result, str]:

    statsd.increment("engine.commands.count", tags=[f"name:{command.name}"])

    if command.args:
        command_full = f"{command.name} {command.args}"
    else:
        command_full = command.name

    commands_restrictions = mergify_config["commands_restrictions"].get(command.name)
    restriction_conditions = None
    if commands_restrictions is not None:
        restriction_conditions = commands_restrictions["conditions"].copy()
        await restriction_conditions([ctxt.pull_request])
    if restriction_conditions is None or restriction_conditions.match:
        conds = conditions.PullRequestRuleConditions(
            await command.action.get_conditions_requirements(ctxt)
        )
        await conds([ctxt.pull_request])
        if conds.match:
            try:
                await command.action.load_context(
                    ctxt,
                    rules.EvaluatedRule(
                        rules.CommandRule(command_full, None, conds, {}, False)
                    ),
                )
            except rules.InvalidPullRequestRule as e:
                result = check_api.Result(
                    check_api.Conclusion.ACTION_REQUIRED,
                    e.reason,
                    e.details,
                )
            else:
                result = await command.action.executor.run()
        elif actions.ActionFlag.ALLOW_AS_PENDING_COMMAND in command.action.flags:
            result = check_api.Result(
                check_api.Conclusion.PENDING,
                "Waiting for conditions to match",
                conds.get_summary(),
            )
        else:
            result = check_api.Result(
                check_api.Conclusion.NEUTRAL,
                "Nothing to do",
                conds.get_summary(),
            )
    else:
        result = check_api.Result(
            check_api.Conclusion.FAILURE,
            "Command disallowed on this pull request",
            restriction_conditions.get_summary(),
        )

    ctxt.log.info(
        "command %s: %s",
        command.name,
        result.conclusion,
        command_full=command_full,
        result=result,
        user=user["login"] if user else None,
    )
    message = prepare_message(command_full, result)
    return result, message


async def handle(
    ctxt: context.Context,
    mergify_config: rules.MergifyConfig,
    comment_command: str,
    user: typing.Optional[github_types.GitHubAccount],
    comment_result: typing.Optional[github_types.GitHubComment] = None,
) -> None:
    # Run command only if this is a pending task or if user have permission to do it.
    if comment_result is None and not user:
        raise RuntimeError("user must be set if comment_result is unset")

    def log(comment_out: str, result: typing.Optional[check_api.Result] = None) -> None:
        ctxt.log.info(
            "ran command",
            user_login=None if user is None else user["login"],
            comment_result=comment_result,
            comment_in=comment_command,
            comment_out=comment_out,
            result=result,
        )

    try:
        command = load_command(mergify_config, comment_command)
    except CommandInvalid as e:
        log(e.message)
        await ctxt.post_comment(e.message)
        return
    except NotACommand:
        return

    if user:
        if (
            user["id"] != ctxt.pull["user"]["id"]
            and user["id"] != config.BOT_USER_ID
            and not await ctxt.repository.has_write_permission(user)
        ) or (
            "queue" in command.name
            and not await ctxt.repository.has_write_permission(user)
        ):
            message = f"@{user['login']} is not allowed to run commands"
            log(message)
            await ctxt.post_comment(message)
            return

    if (
        ctxt.configuration_changed
        and actions.ActionFlag.ALLOW_ON_CONFIGURATION_CHANGED
        not in command.action.flags
    ):
        message = CONFIGURATION_CHANGE_MESSAGE
        log(message)
        await ctxt.post_comment(message)
        return

    if command.name != "refresh" and ctxt.is_merge_queue_pr():
        log(MERGE_QUEUE_COMMAND_MESSAGE)
        await ctxt.post_comment(MERGE_QUEUE_COMMAND_MESSAGE)
        return

    result, message = await run_command(ctxt, mergify_config, command, user)

    log(message, result)
    if comment_result is None:
        await ctxt.post_comment(message)
    elif message != comment_result["body"]:
        await ctxt.edit_comment(comment_result["id"], message)
