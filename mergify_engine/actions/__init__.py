# -*- encoding: utf-8 -*-
#
#  Copyright © 2018—2020 Mergify SAS
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


import abc
import dataclasses
import enum
import importlib.metadata
import typing

import voluptuous

from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import rules
from mergify_engine.rules import conditions


CANCELLED_CHECK_REPORT = check_api.Result(
    check_api.Conclusion.CANCELLED,
    "The rule doesn't match anymore",
    "This action has been cancelled.",
)


PluginGroupT = typing.Literal["mergify_commands", "mergify_actions"]


class PluginClassT(typing.TypedDict, total=False):
    mergify_actions: typing.Dict[str, typing.Type["Action"]]
    mergify_commands: typing.Dict[str, typing.Type["Action"]]


_CLASSES: PluginClassT = {}

RawConfigT = typing.Dict[str, typing.Any]


@enum.unique
class ActionFlag(enum.Flag):
    NONE = 0
    # The action run()/cancel() is executed whatever the previous state was
    ALWAYS_RUN = enum.auto()
    # The action can be run when the Mergify configuration change
    ALLOW_ON_CONFIGURATION_CHANGED = enum.auto()
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


def get_classes(group: PluginGroupT) -> typing.Dict[str, typing.Type["Action"]]:
    if group not in _CLASSES:
        _CLASSES[group] = {
            ep.name: ep.load() for ep in importlib.metadata.entry_points(group=group)
        }
    return _CLASSES[group]


def get_action_schemas() -> typing.Dict[str, typing.Type["Action"]]:
    return {
        name: voluptuous.Coerce(obj)
        for name, obj in get_classes("mergify_actions").items()
    }


def get_commands() -> typing.Dict[str, typing.Type["Action"]]:
    return {name: obj for name, obj in get_classes("mergify_commands").items()}


ActionT = typing.TypeVar("ActionT")
ActionExecutorConfigT = typing.TypeVar("ActionExecutorConfigT")


@dataclasses.dataclass  # type: ignore[misc]
class ActionExecutor(abc.ABC, typing.Generic[ActionT, ActionExecutorConfigT]):
    ctxt: "context.Context"
    rule: "rules.EvaluatedRule"
    config: ActionExecutorConfigT

    @abc.abstractmethod
    async def run(self) -> check_api.Result:  # pragma: no cover
        ...

    @abc.abstractmethod
    async def cancel(self) -> check_api.Result:  # pragma: no cover
        ...

    @property
    def silenced_conclusion(self) -> typing.Tuple[check_api.Conclusion, ...]:
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
        action: "ActionT",
        ctxt: "context.Context",
        rule: "rules.EvaluatedRule",
    ) -> "ActionExecutor[ActionT, ActionExecutorConfigT]":
        ...


@dataclasses.dataclass
class BackwardCompatActionExecutor(ActionExecutor["BackwardCompatAction", RawConfigT]):
    action: "BackwardCompatAction"

    async def run(self) -> check_api.Result:  # pragma: no cover
        return await self.action.run(self.ctxt, self.rule)

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return await self.action.cancel(self.ctxt, self.rule)

    @property
    def silenced_conclusion(self) -> typing.Tuple[check_api.Conclusion, ...]:
        return self.action.silenced_conclusion

    @classmethod
    async def create(
        cls,
        action: "BackwardCompatAction",
        ctxt: "context.Context",
        rule: "rules.EvaluatedRule",
    ) -> "ActionExecutor[BackwardCompatAction, RawConfigT]":
        return cls(ctxt, rule, action.config, action)


class ActionExecutorProtocol(typing.Protocol):
    ctxt: "context.Context"
    rule: "rules.EvaluatedRule"

    @abc.abstractmethod
    async def run(self) -> check_api.Result:  # pragma: no cover
        ...

    @abc.abstractmethod
    async def cancel(self) -> check_api.Result:  # pragma: no cover
        ...

    @property
    def silenced_conclusion(self) -> typing.Tuple[check_api.Conclusion, ...]:
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
        action: "Action",
        ctxt: "context.Context",
        rule: "rules.EvaluatedRule",
    ) -> "ActionExecutorProtocol":
        ...


ValidatorT = typing.Dict[voluptuous.Required, typing.Any]


@dataclasses.dataclass  # type: ignore[misc]
class Action(abc.ABC):
    raw_config_: dataclasses.InitVar[RawConfigT | None] = dataclasses.field(
        default=None
    )
    raw_config: RawConfigT = dataclasses.field(init=False)
    config: RawConfigT = dataclasses.field(init=False)

    flags: typing.ClassVar[ActionFlag] = ActionFlag.NONE

    @property
    @abc.abstractmethod
    def validator(self) -> ValidatorT:
        ...

    executor: ActionExecutorProtocol = dataclasses.field(init=False)
    # NOTE(sileht): mypy didn't handle thing like typing.Type[ActionExecutorProtocol]
    executor_class: typing.ClassVar[typing.Any] = BackwardCompatActionExecutor

    def __post_init__(self, raw_config_: RawConfigT | None) -> None:
        self.raw_config = raw_config_ or {}
        self.config = voluptuous.Schema(self.validator)(self.raw_config)

    async def load_context(
        self, ctxt: context.Context, rule: "rules.EvaluatedRule"
    ) -> None:
        self.executor = await self.executor_class.create(self, ctxt, rule)

    def validate_config(
        self, mergify_config: "rules.MergifyConfig"
    ) -> None:  # pragma: no cover
        pass

    @staticmethod
    def command_to_config(string: str) -> typing.Dict[str, typing.Any]:
        """Convert string to dict config"""
        return {}

    async def get_conditions_requirements(
        self,
        ctxt: context.Context,
    ) -> typing.List[
        typing.Union[conditions.RuleConditionGroup, conditions.RuleCondition]
    ]:
        return []


class BackwardCompatAction(Action):
    @property
    def silenced_conclusion(self) -> typing.Tuple[check_api.Conclusion, ...]:
        # Be default, we create check-run only on failure, CANCELLED is not a
        # failure it's part of the expected state when the conditions that
        # trigger the action didn't match anymore
        return (
            check_api.Conclusion.SUCCESS,
            check_api.Conclusion.CANCELLED,
            check_api.Conclusion.PENDING,
        )

    @staticmethod
    async def wanted_users(
        ctxt: context.Context, users: typing.List[str]
    ) -> typing.Set[str]:
        wanted = set()
        for user in set(users):
            try:
                user = await ctxt.pull_request.render_template(user)
            except context.RenderTemplateFailure:
                # NOTE: this should never happen since
                # the template is validated when parsing the config 🤷
                continue
            else:
                wanted.add(user)

        return wanted

    @abc.abstractmethod
    async def run(
        self, ctxt: context.Context, rule: "rules.EvaluatedRule"
    ) -> check_api.Result:  # pragma: no cover
        ...

    @abc.abstractmethod
    async def cancel(
        self, ctxt: context.Context, rule: "rules.EvaluatedRule"
    ) -> check_api.Result:  # pragma: no cover
        ...
