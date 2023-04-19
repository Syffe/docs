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
from mergify_engine import context
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine import settings
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.rules import conditions as conditions_mod
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import pull_request_rules as prr_config


LOG = daiquiri.getLogger(__name__)

COMMAND_MATCHER = re.compile(
    rf"^(?:\s*)(?:{settings.GITHUB_URL.rstrip('/')}/|@)Mergify(?:|io) (\w*)(.*)",
    re.IGNORECASE,
)

MERGE_QUEUE_COMMAND_MESSAGE = "Command not allowed on merge queue pull request."
UNKNOWN_COMMAND_MESSAGE = "Sorry but I didn't understand the command. Please consult [the commands documentation](https://docs.mergify.com/commands.html) \U0001F4DA."
INVALID_COMMAND_ARGS_MESSAGE = "Sorry but I didn't understand the arguments of the command `{command}`. Please consult [the commands documentation](https://docs.mergify.com/commands/) \U0001F4DA."


@dataclasses.dataclass
class CommandMixin:
    name: prr_config.PullRequestRuleName
    command_args: str

    def get_command(self) -> str:
        if self.command_args:
            return f"{self.name} {self.command_args}"
        return self.name


@dataclasses.dataclass
class Command(CommandMixin):
    name: prr_config.PullRequestRuleName
    command_args: str
    action: actions.Action

    def __str__(self) -> str:
        return self.get_command()


@dataclasses.dataclass
class CommandInvalid(Exception, CommandMixin):
    name: prr_config.PullRequestRuleName
    command_args: str
    message: str


@dataclasses.dataclass
class NotACommand(Exception):
    message: str


class CommandPayload(typing.TypedDict):
    command: str
    conclusion: str


@dataclasses.dataclass
class CommandState:
    command: str
    conclusion: check_api.Conclusion
    github_comment_source: github_types.GitHubComment
    github_comment_result: github_types.GitHubComment | None


def prepare_message(command: Command | CommandInvalid, result: check_api.Result) -> str:
    # NOTE: Do not serialize this with Mergify JSON encoder:
    # we don't want to allow loading/unloading weird value/classes
    # this could be modified by a user, so we keep it really straightforward
    payload = CommandPayload(
        {
            "command": command.get_command(),
            "conclusion": result.conclusion.value,
        }
    )
    details = ""
    if result.summary:
        details = f"<details>\n\n{result.summary}\n\n</details>\n"

    message = f"""> {command.get_command()}

#### {result.conclusion.emoji} {result.title}

{details}

"""

    message += utils.get_mergify_payload(typing.cast(dict[str, typing.Any], payload))
    return message


def load_command(
    mergify_config: mergify_conf.MergifyConfig,
    message: str,
) -> Command:
    """Load an action from a message."""
    action_classes = actions.get_commands()
    match = COMMAND_MATCHER.search(message)

    if not match:
        raise NotACommand(
            "Comment contains '@Mergify/io' tag but is not aimed to be executed as a command"
        )

    action_name = prr_config.PullRequestRuleName(match[1])
    command_args = match[2].strip()

    if action_name in action_classes:
        action_class = action_classes[action_name]

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
                action_name,
                command_args,
                INVALID_COMMAND_ARGS_MESSAGE.format(command=action_name),
            )
        return Command(action_name, command_args, action)

    raise CommandInvalid(action_name, command_args, UNKNOWN_COMMAND_MESSAGE)


@exceptions.log_and_ignore_exception("commands_runner.on_each_event failed")
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


class LastUpdatedOrderedDict(collections.OrderedDict[str, CommandState]):
    def __setitem__(self, key: str, value: CommandState) -> None:
        super().__setitem__(key, value)
        self.move_to_end(key)


class NoCommandStateFound(Exception):
    pass


def extract_command_state(
    comment: github_types.GitHubComment,
    mergify_bot: github_types.GitHubAccount,
    pendings: dict[str, CommandState],
) -> CommandState:
    match = COMMAND_MATCHER.search(comment["body"])

    # Looking for potential first command run
    if match:
        return CommandState(
            f"{match[1]} {match[2].strip()}".strip(),
            conclusion=check_api.Conclusion.PENDING,
            github_comment_source=comment,
            github_comment_result=None,
        )

    # Looking for Mergify command result comment
    if comment["user"]["id"] != mergify_bot["id"]:
        raise NoCommandStateFound()

    payload = typing.cast(
        CommandPayload,
        utils.get_hidden_payload_from_comment_body(comment["body"]),
    )

    if not payload:
        raise NoCommandStateFound()

    if (
        not isinstance(payload, dict)
        or "command" not in payload
        or "conclusion" not in payload
    ):
        # backward compat <= 7.2.1
        LOG.warning("got command with invalid payload", payload=payload)  # type: ignore[unreachable]
        raise NoCommandStateFound()

    try:
        conclusion = check_api.Conclusion(payload["conclusion"])
    except ValueError:
        LOG.error("Unable to load conclusions %s", payload["conclusion"])
        raise NoCommandStateFound()

    previous_state = pendings.get(payload["command"])
    if previous_state is None:
        LOG.warning("Unable to find initial command comment")
        raise NoCommandStateFound()

    return CommandState(
        payload["command"],
        conclusion=conclusion,
        github_comment_source=previous_state.github_comment_source,
        github_comment_result=comment,
    )


async def run_commands_tasks(
    ctxt: context.Context, mergify_config: mergify_conf.MergifyConfig
) -> None:
    mergify_bot = await github.GitHubAppInfo.get_bot(
        ctxt.repository.installation.redis.cache
    )

    pendings = LastUpdatedOrderedDict()
    async for attempt in tenacity.AsyncRetrying(
        stop=tenacity.stop_after_attempt(2),
        wait=tenacity.wait_exponential(0.2),
        retry=tenacity.retry_if_exception_type(http.HTTPNotFound),
        reraise=True,
    ):
        with attempt:
            comments: list[github_types.GitHubComment] = [
                c
                async for c in ctxt.client.items(
                    f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments",
                    resource_name="comments",
                    page_limit=20,
                )
            ]

    for comment in comments:
        try:
            state = extract_command_state(comment, mergify_bot, pendings)
        except NoCommandStateFound:
            continue

        if state.conclusion == check_api.Conclusion.PENDING:
            pendings[state.command] = state
        elif state.command in pendings:
            del pendings[state.command]

    for pending, state in pendings.items():
        try:
            command = load_command(mergify_config, f"@Mergifyio {pending}")
        except CommandInvalid as e:
            if state.github_comment_result is None:
                result = check_api.Result(check_api.Conclusion.FAILURE, e.message, "")
                await post_result(ctxt, e, state, result)
            else:
                ctxt.log.error(
                    "Unexpected command string, it was valid in the past but not anymore",
                    command_state=state,
                    exc_info=True,
                )

        except NotACommand:
            if state.github_comment_result is not None:
                ctxt.log.error(
                    "Unexpected command string, it was valid in the past but not anymore",
                    command_state=state,
                    exc_info=True,
                )
        else:
            if not ctxt.subscription.has_feature(
                command.action.required_feature_for_command
            ):
                await post_result(
                    ctxt,
                    command,
                    state,
                    check_api.Result(
                        check_api.Conclusion.ACTION_REQUIRED,
                        f"Cannot use the command `{pending}`",
                        ctxt.subscription.missing_feature_reason(
                            ctxt.pull["base"]["repo"]["owner"]["login"]
                        ),
                    ),
                )
                continue

            result = await run_command(ctxt, mergify_config, command, state)
            await post_result(ctxt, command, state, result)


async def post_result(
    ctxt: context.Context,
    command: Command | CommandInvalid,
    state: CommandState,
    result: check_api.Result,
) -> None:
    message = prepare_message(command, result)
    ctxt.log.info(
        "command %s: %s",
        command.name,
        result.conclusion,
        command_full=str(command),
        result=result,
        user_login=state.github_comment_source["user"]["login"]
        if state.github_comment_source
        else "<unavailable>",
        comment_out=message,
        comment_result=state.github_comment_result,
    )

    if state.github_comment_result is None:
        await ctxt.post_comment(message)
    elif message != state.github_comment_result["body"]:
        await ctxt.edit_comment(state.github_comment_result["id"], message)


async def run_command(
    ctxt: context.Context,
    mergify_config: mergify_conf.MergifyConfig,
    command: Command,
    state: CommandState,
) -> check_api.Result:
    statsd.increment("engine.commands.count", tags=[f"name:{command.name}"])
    try:
        await check_init_command_run(ctxt, mergify_config, command, state)
    except CommandNotAllowed as e:
        return check_api.Result(check_api.Conclusion.FAILURE, e.title, e.message)

    conds = conditions_mod.PullRequestRuleConditions(
        await command.action.get_conditions_requirements(ctxt)
    )
    await conds([ctxt.pull_request])
    if conds.match:
        try:
            await command.action.load_context(
                ctxt,
                prr_config.EvaluatedPullRequestRule(
                    prr_config.CommandRule(
                        prr_config.PullRequestRuleName(str(command)),
                        None,
                        conds,
                        {},
                        False,
                        state.github_comment_source["user"],
                    )
                ),
            )
        except actions.InvalidDynamicActionConfiguration as e:
            return check_api.Result(
                check_api.Conclusion.ACTION_REQUIRED,
                e.reason,
                e.details,
            )
        else:
            return await command.action.executor.run()
    elif actions.ActionFlag.ALLOW_AS_PENDING_COMMAND in command.action.flags:
        return check_api.Result(
            check_api.Conclusion.PENDING,
            "Waiting for conditions to match",
            conds.get_summary(),
        )
    else:
        return check_api.Result(
            check_api.Conclusion.NEUTRAL,
            "Nothing to do",
            conds.get_summary(),
        )


@dataclasses.dataclass
class CommandNotAllowed(Exception):
    title: str
    message: str


@dataclasses.dataclass
class CommandNotAllowedDueToRestriction(CommandNotAllowed):
    title: str = dataclasses.field(
        init=False,
        default="Command disallowed due to [command restrictions](https://docs.mergify.com/configuration/#commands-restrictions) in the Mergify configuration.",
    )
    message: str = dataclasses.field(init=False)
    conditions: conditions_mod.PullRequestRuleConditions

    def __post_init__(self) -> None:
        self.message = self.conditions.get_summary()


async def check_command_restrictions(
    ctxt: context.Context,
    mergify_config: mergify_conf.MergifyConfig,
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
            raise CommandNotAllowedDueToRestriction(restriction_conditions)


async def check_init_command_run(
    ctxt: context.Context,
    mergify_config: mergify_conf.MergifyConfig,
    command: Command,
    state: CommandState,
) -> None:
    # Not a initial command run
    if state.github_comment_result is not None:
        return

    if command.name != "refresh" and ctxt.is_merge_queue_pr():
        raise CommandNotAllowed(MERGE_QUEUE_COMMAND_MESSAGE, "")

    await check_command_restrictions(
        ctxt,
        mergify_config,
        command,
        state.github_comment_source["user"],
    )
