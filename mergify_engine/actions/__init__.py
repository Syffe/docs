from __future__ import annotations

import abc
import dataclasses
import enum
import importlib.metadata
import typing

import voluptuous

from mergify_engine import check_api
from mergify_engine import subscription


if typing.TYPE_CHECKING:
    from mergify_engine import context
    from mergify_engine.engine import commands_runner
    from mergify_engine.rules import conditions
    from mergify_engine.rules.config import pull_request_rules as prr_config


CANCELLED_CHECK_REPORT = check_api.Result(
    check_api.Conclusion.CANCELLED,
    "The pull request rule doesn't match anymore",
    "This action has been cancelled.",
)


PluginGroupT = typing.Literal["mergify_commands", "mergify_actions"]


class PluginClassT(typing.TypedDict, total=False):
    mergify_actions: dict[str, type[Action]]
    mergify_commands: dict[str, type[Action]]


_CLASSES: PluginClassT = {}

RawConfigT = dict[str, typing.Any]


@enum.unique
class ActionFlag(enum.Flag):
    NONE = 0
    # The action run()/cancel() is executed whatever the previous state was
    ALWAYS_RUN = enum.auto()
    # Allow to rerun an action if it's part of another rule
    DISALLOW_RERUN_ON_OTHER_RULES = enum.auto()
    # This makes checks created by mergify retriggering Mergify, beware to
    # not create something that endup with a infinite loop of events
    ALLOW_RETRIGGER_MERGIFY = enum.auto()
    # Allow to return this command as pending if does not match its conditions
    # requirements.
    ALLOW_AS_PENDING_COMMAND = enum.auto()
    # Once succeed the action must not be ran anymore
    SUCCESS_IS_FINAL_STATE = enum.auto()
    # A command in PENDING status will be canceled if the same command
    # with different args is executed
    SAME_COMMAND_WITH_DIFFERENT_ARGS_CANCEL_PENDING_STATUS = enum.auto()


def get_classes(group: PluginGroupT) -> dict[str, type[Action]]:
    if group not in _CLASSES:
        _CLASSES[group] = {
            ep.name: ep.load() for ep in importlib.metadata.entry_points(group=group)
        }
    return _CLASSES[group]


def get_action_schemas() -> dict[str, type[Action]]:
    return {
        name: voluptuous.Coerce(obj)
        for name, obj in get_classes("mergify_actions").items()
    }


def get_commands() -> dict[str, type[Action]]:
    return get_classes("mergify_commands")


ActionT = typing.TypeVar("ActionT")
ActionExecutorConfigT = typing.TypeVar("ActionExecutorConfigT")


@dataclasses.dataclass
class InvalidDynamicActionConfiguration(Exception):
    rule: prr_config.EvaluatedPullRequestRule
    action: Action
    reason: str
    details: str

    @property
    def action_name(self) -> str:
        global _CLASSES
        groups: tuple[PluginGroupT, ...] = ("mergify_actions", "mergify_commands")
        for group in groups:
            for name, obj in get_classes(group).items():
                if isinstance(self.action, obj):
                    return name
        raise RuntimeError(f"Can't find action {self.action!r}")

    def get_summary(self) -> str:
        return f"""In the rule `{self.rule.name}`, the action `{self.action_name}` configuration is invalid:
{self.reason}
{self.details}
"""


@dataclasses.dataclass
class ActionExecutor(abc.ABC, typing.Generic[ActionT, ActionExecutorConfigT]):
    ctxt: context.Context
    rule: prr_config.EvaluatedPullRequestRule
    config: ActionExecutorConfigT
    config_hidden_from_simulator: typing.ClassVar[tuple[str, ...]] = ()

    @abc.abstractmethod
    async def run(
        self,
    ) -> check_api.Result | commands_runner.FollowUpCommand:  # pragma: no cover
        ...

    @abc.abstractmethod
    async def cancel(self) -> check_api.Result:  # pragma: no cover
        ...

    @property
    def silenced_conclusion(self) -> tuple[check_api.Conclusion, ...]:
        # Be default, we create check-run only on failure, CANCELLED is not a
        # failure it's part of the expected state when the conditions that
        # trigger the action didn't match anyore
        return (
            check_api.Conclusion.SUCCESS,
            check_api.Conclusion.CANCELLED,
            check_api.Conclusion.PENDING,
        )

    @classmethod
    @abc.abstractmethod
    async def create(
        cls,
        # FIXME(sileht): pass just RawConfigT instead of the "Action"
        action: ActionT,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> ActionExecutor[ActionT, ActionExecutorConfigT]:
        ...


class ActionExecutorProtocol(typing.Protocol):
    ctxt: context.Context
    rule: prr_config.EvaluatedPullRequestRule
    config: dict[str, typing.Any]
    config_hidden_from_simulator: typing.ClassVar[tuple[str, ...]]

    @abc.abstractmethod
    async def run(self) -> check_api.Result:  # pragma: no cover
        ...

    @abc.abstractmethod
    async def cancel(self) -> check_api.Result:  # pragma: no cover
        ...

    @property
    def silenced_conclusion(self) -> tuple[check_api.Conclusion, ...]:
        # Be default, we create check-run only on failure, CANCELLED is not a
        # failure it's part of the expected state when the conditions that
        # trigger the action didn't match anyore
        return (
            check_api.Conclusion.SUCCESS,
            check_api.Conclusion.CANCELLED,
            check_api.Conclusion.PENDING,
        )

    @classmethod
    async def create(
        cls,
        # FIXME(sileht): pass just RawConfigT instead of the "Action"
        action: Action,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> ActionExecutorProtocol:
        ...


ValidatorT = dict[voluptuous.Required, typing.Any]


@dataclasses.dataclass
class Action(abc.ABC):
    raw_config_: dataclasses.InitVar[RawConfigT | None] = dataclasses.field(
        default=None,
    )
    raw_config: RawConfigT = dataclasses.field(init=False)
    config: RawConfigT = dataclasses.field(init=False)

    flags: typing.ClassVar[ActionFlag] = ActionFlag.NONE

    @property
    @abc.abstractmethod
    def validator(self) -> ValidatorT:
        ...

    executor: ActionExecutorProtocol = dataclasses.field(init=False, repr=False)
    # NOTE(sileht): mypy didn't handle inheritance for thing like type[ActionExecutorProtocol]
    executor_class: typing.ClassVar[typing.Any]

    # Default command restrictions
    # If the command does not define a default command restriction (e.g. a
    # developper oversight), this restriction apply.
    default_restrictions: typing.ClassVar[list[str]] = ["sender-permission>=write"]

    required_feature_for_command: typing.ClassVar[
        subscription.Features
    ] = subscription.Features.WORKFLOW_AUTOMATION

    def __post_init__(self, raw_config_: RawConfigT | None) -> None:
        self.raw_config = raw_config_ or {}
        self.config = voluptuous.Schema(
            voluptuous.All(
                {voluptuous.Extra: object},  # just ensure first it's a dict
                self.validator,
            ),
        )(self.raw_config)

    async def load_context(
        self,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> None:
        self.executor = await self.executor_class.create(self, ctxt, rule)

    @staticmethod
    def command_to_config(string: str) -> dict[str, typing.Any]:
        """Convert string to dict config"""
        return {}

    async def get_conditions_requirements(
        self,
        ctxt: context.Context,
    ) -> list[conditions.RuleConditionNode]:
        return []
