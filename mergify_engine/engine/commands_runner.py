import collections
import dataclasses
import re
import typing

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
import first
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
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules import conditions as conditions_mod
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import pull_request_rules as prr_config


LOG = daiquiri.getLogger(__name__)

COMMAND_MATCHER = re.compile(
    rf"^(?:\s*)(?:{settings.GITHUB_URL}/|@)Mergify(?:|io) (\w*)(.*)",
    re.IGNORECASE,
)

MERGE_QUEUE_COMMAND_MESSAGE = "Command not allowed on merge queue pull request."
UNKNOWN_COMMAND_MESSAGE = "Sorry but I didn't understand the command. Please consult [the commands documentation](https://docs.mergify.com/commands.html) \U0001F4DA."
INVALID_COMMAND_ARGS_MESSAGE = "Sorry but I didn't understand the arguments of the command `{command}`. Please consult [the commands documentation](https://docs.mergify.com/commands/) \U0001F4DA."


@dataclasses.dataclass
class FollowUpCommand:
    command_str: str
    previous_command_result: check_api.Result


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


class CommandPayloadBase(typing.TypedDict):
    command: str
    conclusion: str
    action_is_running: bool


class CommandPayload(CommandPayloadBase, total=False):
    followup_of_command: str


class CommandExecutionState(typing.NamedTuple):
    result: check_api.Result
    action_is_running: bool


@dataclasses.dataclass
class CommandState:
    command: str
    conclusion: check_api.Conclusion
    github_comment_source: github_types.GitHubComment
    github_comment_result: github_types.GitHubComment | None
    action_is_running: bool
    followup_of_command: str | None = dataclasses.field(default=None)


def prepare_message(
    command: str,
    result: check_api.Result,
    action_is_running: bool,
    followup_of_command: str | None = None,
) -> str:
    # NOTE: Do not serialize this with Mergify JSON encoder:
    # we don't want to allow loading/unloading weird value/classes
    # this could be modified by a user, so we keep it really straightforward
    payload = CommandPayload(
        {
            "command": command,
            "conclusion": result.conclusion.value,
            "action_is_running": action_is_running,
        }
    )
    if followup_of_command is not None:
        payload["followup_of_command"] = followup_of_command

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


# NOTE(sileht): Unlike the Python dict, this one move to the end key that have been updated
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
    finished_commands: dict[str, CommandState],
) -> CommandState:
    match = COMMAND_MATCHER.search(comment["body"])

    # Looking for potential first command run
    if match:
        return CommandState(
            f"{match[1]} {match[2].strip()}".strip(),
            conclusion=check_api.Conclusion.PENDING,
            github_comment_source=comment,
            github_comment_result=None,
            action_is_running=False,
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

    # TODO(Syffe): remove this once every command has been updated with new payload format
    # backward compat, we need to set a value for old payloads
    action_is_running = payload.get("action_is_running", False)

    try:
        conclusion = check_api.Conclusion(payload["conclusion"])
    except ValueError:
        LOG.error("Unable to load conclusions %s", payload["conclusion"])
        raise NoCommandStateFound()

    previous_state = pendings.get(payload["command"])
    if previous_state is None:
        if (followup_of_command := payload.get("followup_of_command")) is not None:
            # This command is a followup of another command, so it has no real github_comment_source.
            # The github_comment_source is the comment of the command that issued the followup.
            # Since the original command would have ended, it won't be in the `pendings`.
            return CommandState(
                payload["command"],
                conclusion=conclusion,
                github_comment_source=finished_commands[
                    followup_of_command
                ].github_comment_source,
                github_comment_result=comment,
                action_is_running=action_is_running,
                followup_of_command=followup_of_command,
            )

        LOG.warning("Unable to find initial command comment")
        raise NoCommandStateFound()

    return CommandState(
        payload["command"],
        conclusion=conclusion,
        github_comment_source=previous_state.github_comment_source,
        github_comment_result=comment,
        action_is_running=action_is_running,
        followup_of_command=payload.get("followup_of_command"),
    )


async def get_pending_commands_to_run_from_comments(
    ctxt: context.Context,
) -> LastUpdatedOrderedDict:
    mergify_bot = await github.GitHubAppInfo.get_bot(
        ctxt.repository.installation.redis.cache
    )
    pendings = LastUpdatedOrderedDict()
    finished_commands = LastUpdatedOrderedDict()

    comments: list[github_types.GitHubComment] = [
        c
        async for c in ctxt.client.items(
            f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments",
            resource_name="comments",
            page_limit=20,
            extensions={"retry": lambda r: r.status_code == 404},
        )
    ]

    for comment in comments:
        try:
            state = extract_command_state(
                comment,
                mergify_bot,
                pendings,
                finished_commands,
            )
        except NoCommandStateFound:
            continue

        if state.conclusion == check_api.Conclusion.PENDING:
            pendings[state.command] = state
        else:
            finished_commands[state.command] = state
            if state.command in pendings:
                del pendings[state.command]

    return pendings


async def pre_commands_run(
    ctxt: context.Context,
    pendings: LastUpdatedOrderedDict,
    mergify_config: mergify_conf.MergifyConfig,
) -> list[tuple[Command | CommandInvalid | NotACommand, CommandState]]:
    loaded_commands: list[
        tuple[Command | CommandInvalid | NotACommand, CommandState]
    ] = []
    seen_commands_names = set()
    for pending, state in pendings.items():
        try:
            command = load_command(mergify_config, f"@Mergifyio {pending}")
        except CommandInvalid as e:
            loaded_commands.append((e, state))
            seen_commands_names.add(e.name)
            continue
        except NotACommand as e:
            loaded_commands.append((e, state))
            continue

        loaded_commands.append((command, state))

        if not (
            command.name in seen_commands_names
            and actions.ActionFlag.SAME_COMMAND_WITH_DIFFERENT_ARGS_CANCEL_PENDING_STATUS
            in command.action.flags
        ):
            seen_commands_names.add(command.name)
            continue

        ctxt.log.info(
            "Command '%s' needs to override a previously loaded %s command",
            command.get_command(),
            command.name,
        )
        try:
            # Make sure the new command has the rights to be executed first,
            # otherwise we could be cancelling actions from unauthorized
            # senders.
            await check_init_command_run(ctxt, mergify_config, command, state)
        except CommandNotAllowed:
            ctxt.log.info(
                "Command '%s' doesn't have the rights to be executed, cancelling overload of previous command.",
                command.get_command(),
            )
            continue

        # A command with different arguments already exist in the commands
        # we already loaded, and the `Action` class of that command says
        # that we need to cancel previous commands.
        # So we need to cancel the previously loaded command in order to
        # then execute the command with different args.
        loaded_command_tuple = first.first(
            tmp_command
            for tmp_command in loaded_commands
            if isinstance(tmp_command[0], Command)
            and tmp_command[0].name == command.name
        )
        if loaded_command_tuple is None:
            raise RuntimeError("Did not found existing command in `loaded_commands`")

        loaded_command, loaded_command_state = loaded_command_tuple
        # mypy doesn't understand that the `if` in the `first.first` make it so that
        # loaded_command is a Command
        loaded_command = typing.cast(Command, loaded_command)
        loaded_commands.remove((loaded_command, loaded_command_state))

        ctxt.log.info("Cancelling pending command '%s'", command.get_command())

        await run_command(
            ctxt,
            mergify_config,
            # NOTE(Greesb): Pass the new `command` instead of the `loaded_command`
            # in order for the `cancel` to see the new requirements of the action.
            # Otherwise the conditions would still match and the `cancel` might not do anything.
            # eg: if we do `queue foo` and then `queue bar`, the requirements of the
            # queue `bar` will be passed to the `cancel` of the queue action which will
            # result in the PR being unqueued.
            command,
            loaded_command_state,
            force_run=True,
            executor_func_to_run="cancel",
        )
        await post_result(
            ctxt,
            loaded_command,
            loaded_command_state,
            CommandExecutionState(
                result=check_api.Result(
                    check_api.Conclusion.CANCELLED,
                    f"Command `{loaded_command.get_command()}` cancelled because of a new `{command.name}` command with different arguments",
                    "",
                ),
                action_is_running=False,
            ),
        )

        seen_commands_names.add(command.name)

    return loaded_commands


async def run_commands_tasks(
    ctxt: context.Context, mergify_config: mergify_conf.MergifyConfig
) -> None:
    pendings = await get_pending_commands_to_run_from_comments(ctxt)

    commands = await pre_commands_run(ctxt, pendings, mergify_config)

    for idx, (command, state) in enumerate(commands):
        if isinstance(command, CommandInvalid):
            if state.github_comment_result is None:
                command_execution_state = CommandExecutionState(
                    result=check_api.Result(
                        check_api.Conclusion.FAILURE, command.message, ""
                    ),
                    action_is_running=False,
                )
                await post_result(ctxt, command, state, command_execution_state)
            else:
                ctxt.log.error(
                    "Unexpected command string, it was valid in the past but not anymore",
                    command_state=state,
                    exc=str(command),
                )

        elif isinstance(command, NotACommand):
            if state.github_comment_result is not None:
                ctxt.log.error(
                    "Unexpected command string, it was valid in the past but not anymore",
                    command_state=state,
                    exc=str(command),
                )
        elif not ctxt.subscription.has_feature(
            command.action.required_feature_for_command
        ):
            await post_result(
                ctxt,
                command,
                state,
                CommandExecutionState(
                    result=check_api.Result(
                        check_api.Conclusion.ACTION_REQUIRED,
                        f"Cannot use the command `{command.get_command()}`",
                        ctxt.subscription.missing_feature_reason(
                            ctxt.pull["base"]["repo"]["owner"]["login"]
                        ),
                    ),
                    action_is_running=False,
                ),
            )
        else:
            command_result = await run_command(ctxt, mergify_config, command, state)
            if isinstance(command_result, CommandExecutionState):
                await post_result(ctxt, command, state, command_result)
                continue

            # isinstance(command_result, FollowUpCommand)
            followup_command_str = command_result.command_str
            await post_result_for_command_with_followup(
                ctxt,
                command,
                state,
                command_result.previous_command_result,
                followup_command_str,
            )

            ctxt.log.info(
                "command '%s' has a follow up command to execute: '%s'",
                command.name,
                followup_command_str,
            )
            followup_command: Command | CommandInvalid | NotACommand
            try:
                followup_command = load_command(
                    mergify_config, f"@Mergifyio {followup_command_str}"
                )
            except CommandInvalid as e:
                followup_command = e
                ctxt.log.error(
                    "CommandInvalid while loading followup command '%s'",
                    followup_command_str,
                    exc_info=True,
                )
            except NotACommand as e:
                followup_command = e
                ctxt.log.error(
                    "NotACommandError while loading followup command '%s'",
                    followup_command_str,
                    exc_info=True,
                )

            followup_command_state = CommandState(
                command=followup_command_str,
                conclusion=check_api.Conclusion.PENDING,
                github_comment_source=state.github_comment_source,
                github_comment_result=None,
                action_is_running=False,
                followup_of_command=str(command),
            )
            commands.insert(idx + 1, (followup_command, followup_command_state))


async def post_result_for_command_with_followup(
    ctxt: context.Context,
    command: Command,
    state: CommandState,
    command_result: check_api.Result,
    followup_command_str: str,
) -> None:
    message = prepare_message(
        command.get_command(),
        command_result,
        True,
    )
    ctxt.log.info(
        "command %s: %s",
        command.name,
        command_result.conclusion,
        command_full=str(command),
        followup_command_str=followup_command_str,
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


async def post_result(
    ctxt: context.Context,
    command: Command | CommandInvalid,
    state: CommandState,
    command_execution_state: CommandExecutionState,
) -> None:
    message = prepare_message(
        command.get_command(),
        command_execution_state.result,
        command_execution_state.action_is_running,
        state.followup_of_command,
    )
    ctxt.log.info(
        "command %s: %s",
        command.name,
        command_execution_state.result.conclusion,
        command_full=str(command),
        result=command_execution_state.result,
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
    force_run: bool = False,
    executor_func_to_run: typing.Literal["run", "cancel"] = "run",
) -> CommandExecutionState | FollowUpCommand:
    statsd.increment("engine.commands.count", tags=[f"name:{command.name}"])
    try:
        await check_init_command_run(ctxt, mergify_config, command, state)
    except CommandNotAllowed as e:
        return CommandExecutionState(
            result=check_api.Result(check_api.Conclusion.FAILURE, e.title, e.message),
            action_is_running=False,
        )

    conds = conditions_mod.PullRequestRuleConditions(
        await command.action.get_conditions_requirements(ctxt)
    )
    await conds([ctxt.pull_request])
    if conds.match or force_run:
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
            return CommandExecutionState(
                result=check_api.Result(
                    check_api.Conclusion.ACTION_REQUIRED,
                    e.reason,
                    e.details,
                ),
                action_is_running=False,
            )
        else:
            # Do not use getattr to keep typing
            if executor_func_to_run == "run":
                command_result = await command.action.executor.run()
                if isinstance(command_result, FollowUpCommand):
                    return command_result

                return CommandExecutionState(
                    result=command_result,
                    action_is_running=True,
                )
            if executor_func_to_run == "cancel":
                return CommandExecutionState(
                    result=await command.action.executor.cancel(),
                    action_is_running=True,
                )
            raise RuntimeError(f"Unhandled executor function '{executor_func_to_run}'")

    elif state.action_is_running:
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
            return CommandExecutionState(
                result=check_api.Result(
                    check_api.Conclusion.ACTION_REQUIRED,
                    e.reason,
                    e.details,
                ),
                action_is_running=True,
            )
        else:
            return CommandExecutionState(
                result=await command.action.executor.cancel(),
                action_is_running=True,
            )

    elif actions.ActionFlag.ALLOW_AS_PENDING_COMMAND in command.action.flags:
        return CommandExecutionState(
            result=check_api.Result(
                check_api.Conclusion.PENDING,
                "Waiting for conditions to match",
                conds.get_summary(),
            ),
            action_is_running=False,
        )
    else:
        return CommandExecutionState(
            result=check_api.Result(
                check_api.Conclusion.NEUTRAL,
                "Nothing to do",
                conds.get_summary(),
            ),
            action_is_running=False,
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

    if command.name != "refresh" and queue_utils.is_merge_queue_pr(ctxt.pull):
        raise CommandNotAllowed(MERGE_QUEUE_COMMAND_MESSAGE, "")

    await check_command_restrictions(
        ctxt,
        mergify_config,
        command,
        state.github_comment_source["user"],
    )
