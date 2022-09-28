import collections
import dataclasses
import re
import typing

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
import tenacity
import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import config
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.rules import conditions as conditions_mod


LOG = daiquiri.getLogger(__name__)

COMMAND_MATCHER = re.compile(
    rf"^(?:{config.GITHUB_URL.rstrip('/')}/|@)Mergify(?:|io) (\w*)(.*)",  # noqa: BLK100
    re.IGNORECASE,
)

MERGE_QUEUE_COMMAND_MESSAGE = "Command not allowed on merge queue pull request."
UNKNOWN_COMMAND_MESSAGE = "Sorry but I didn't understand the command. Please consult [the commands documentation](https://docs.mergify.com/commands.html) \U0001F4DA."
INVALID_COMMAND_ARGS_MESSAGE = "Sorry but I didn't understand the arguments of the command `{command}`. Please consult [the commands documentation](https://docs.mergify.com/commands/) \U0001F4DA."  # noqa
CONFIGURATION_CHANGE_MESSAGE = (
    "Sorry but this command cannot run when the configuration is updated"
)


@dataclasses.dataclass
class Command:
    name: str
    args: str
    action: actions.Action

    def __str__(self) -> str:
        if self.args:
            return f"{self.name} {self.args}"
        else:
            return self.name


@dataclasses.dataclass
class CommandInvalid(Exception):
    message: str


@dataclasses.dataclass
class NotACommand(Exception):
    message: str


class CommandPayload(typing.TypedDict):
    command: str
    conclusion: str


def prepare_message(command: Command, result: check_api.Result) -> str:
    # NOTE: Do not serialize this with Mergify JSON encoder:
    # we don't want to allow loading/unloading weird value/classes
    # this could be modified by a user, so we keep it really straightforward
    payload = CommandPayload(
        {
            "command": str(command),
            "conclusion": result.conclusion.value,
        }
    )
    details = ""
    if result.summary:
        details = f"<details>\n\n{result.summary}\n\n</details>\n"

    message = f"""> {command}

#### {result.conclusion.emoji} {result.title}

{details}

"""

    message += utils.get_mergify_payload(typing.cast(dict[str, typing.Any], payload))
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
    for attempt in tenacity.AsyncRetrying(  # type: ignore[attr-defined]
        stop=tenacity.stop_after_attempt(2),  # type: ignore[attr-defined]
        wait=tenacity.wait_exponential(0.2),  # type: ignore[attr-defined]
        retry=tenacity.retry_if_exception_type(http.HTTPNotFound),  # type: ignore[attr-defined]
        reraise=True,
    ):
        with attempt:
            comments: typing.List[github_types.GitHubComment] = [
                c
                async for c in ctxt.client.items(
                    f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments",
                    resource_name="comments",
                    page_limit=20,
                )
            ]

    for comment in comments:
        if comment["user"]["id"] != config.BOT_USER_ID:
            continue

        payload = typing.cast(
            CommandPayload, utils.get_hidden_payload_from_comment_body(comment["body"])
        )

        if not payload:
            continue

        if (
            not isinstance(payload, dict)
            or "command" not in payload
            or "conclusion" not in payload
        ):
            LOG.warning("got command with invalid payload", payload=payload)
            continue

        command_str = payload["command"]
        conclusion_str = payload["conclusion"]

        try:
            conclusion = check_api.Conclusion(conclusion_str)
        except ValueError:
            LOG.error("Unable to load conclusions %s", conclusion_str)
            continue

        if conclusion == check_api.Conclusion.PENDING:
            pendings[command_str] = comment
        elif command_str in pendings:
            del pendings[command_str]

    for pending, comment in pendings.items():
        try:
            command = load_command(mergify_config, f"@Mergifyio {pending}")
        except (CommandInvalid, NotACommand):
            ctxt.log.error(
                "Unexpected command string, it was valid in the past but not anymore",
                exc_info=True,
            )
            continue
        await run_command(ctxt, mergify_config, command, comment_result=comment)


async def run_command(
    ctxt: context.Context,
    mergify_config: rules.MergifyConfig,
    command: Command,
    comment_result: typing.Optional[github_types.GitHubComment] = None,
) -> None:
    statsd.increment("engine.commands.count", tags=[f"name:{command.name}"])

    conds = conditions_mod.PullRequestRuleConditions(
        await command.action.get_conditions_requirements(ctxt)
    )
    await conds([ctxt.pull_request])
    if conds.match:
        try:
            await command.action.load_context(
                ctxt,
                rules.EvaluatedRule(
                    rules.CommandRule(str(command), None, conds, {}, False)
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
    ctxt.log.info(
        "command %s: %s",
        command.name,
        result.conclusion,
        command_full=str(command),
        result=result,
    )
    message = prepare_message(command, result)

    ctxt.log.info(
        "command ran",
        command=str(command),
        result=result,
        comment_result=comment_result,
    )

    if comment_result is None:
        await ctxt.post_comment(message)
    elif message != comment_result["body"]:
        await ctxt.edit_comment(comment_result["id"], message)


@dataclasses.dataclass
class CommandNotAllowed(Exception):
    conditions: conditions_mod.PullRequestRuleConditions


async def check_command_restrictions(
    ctxt: context.Context,
    mergify_config: rules.MergifyConfig,
    command: Command,
    user: github_types.GitHubAccount,
) -> None:
    commands_restrictions = mergify_config["commands_restrictions"].get(command.name)

    if commands_restrictions is not None:
        restriction_conditions = commands_restrictions["conditions"].copy()
        rules.apply_configure_filter(ctxt.repository, restriction_conditions)
        user_permission = await ctxt.repository.get_user_permission(user)
        command_pull_request = context.CommandPullRequest(
            ctxt, user["login"], user_permission
        )
        await restriction_conditions([command_pull_request])

        if not restriction_conditions.match:
            raise CommandNotAllowed(restriction_conditions)


async def handle(
    ctxt: context.Context,
    mergify_config: rules.MergifyConfig,
    comment_command: str,
    user: github_types.GitHubAccount,
) -> None:
    def log(comment_out: str) -> None:
        ctxt.log.info(
            "handle command",
            user_login=user["login"],
            comment_in=comment_command,
            comment_out=comment_out,
        )

    try:
        command = load_command(mergify_config, comment_command)
    except CommandInvalid as e:
        log(e.message)
        await ctxt.post_comment(e.message)
        return
    except NotACommand:
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

    # FIXME(sileht): should be done with restriction_conditions: MRGFY-1405
    if (
        user["id"] != ctxt.pull["user"]["id"]
        and user["id"] != config.BOT_USER_ID
        and not await ctxt.repository.has_write_permission(user)
    ) or (
        "queue" in command.name and not await ctxt.repository.has_write_permission(user)
    ):
        message = f"@{user['login']} is not allowed to run commands"
        log(message)
        await ctxt.post_comment(message)
        return

    try:
        await check_command_restrictions(ctxt, mergify_config, command, user)
    except CommandNotAllowed as e:
        result = check_api.Result(
            check_api.Conclusion.FAILURE,
            "Command disallowed due to [command restrictions](https://docs.mergify.com/configuration/#commands-restrictions) in the Mergify configuration.",
            e.conditions.get_summary(),
        )
        message = prepare_message(command, result)
        log(message)
        await ctxt.post_comment(message)
        return

    await run_command(ctxt, mergify_config, command)
