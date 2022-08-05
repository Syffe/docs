# -*- encoding: utf-8 -*-
#
# Copyright © 2018-2021 Mergify SAS
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
import dataclasses
import datetime
import functools
import itertools
import operator
import typing

import daiquiri
from ddtrace import tracer
import voluptuous
import yaml

from mergify_engine import actions as actions_mod
from mergify_engine import config
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine.rules import conditions as conditions_mod
from mergify_engine.rules import live_resolvers
from mergify_engine.rules import types


LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class InvalidPullRequestRule(Exception):
    reason: str
    details: str


class DisabledDict(typing.TypedDict):
    reason: str


@dataclasses.dataclass
class PullRequestRule:
    name: str
    disabled: typing.Union[DisabledDict, None]
    conditions: conditions_mod.PullRequestRuleConditions
    actions: typing.Dict[str, actions_mod.Action]
    hidden: bool = False
    from_command: bool = False

    class T_from_dict_required(typing.TypedDict):
        name: str
        disabled: typing.Union[DisabledDict, None]
        conditions: conditions_mod.PullRequestRuleConditions
        actions: typing.Dict[str, actions_mod.Action]

    class T_from_dict(T_from_dict_required, total=False):
        hidden: bool

    @classmethod
    def from_dict(cls, d: T_from_dict) -> "PullRequestRule":
        return cls(**d)

    def get_check_name(self, action: str) -> str:
        return f"Rule: {self.name} ({action})"

    def get_signal_trigger(self) -> str:
        return f"Rule: {self.name}"

    async def evaluate(
        self, pulls: typing.List[context.BasePullRequest]
    ) -> "EvaluatedRule":
        evaluated_rule = typing.cast(EvaluatedRule, self)
        await evaluated_rule.conditions(pulls)
        for action in self.actions.values():
            await action.load_context(
                typing.cast(context.PullRequest, pulls[0]).context, evaluated_rule
            )
        return evaluated_rule


@dataclasses.dataclass
class CommandRule(PullRequestRule):
    def get_signal_trigger(self) -> str:
        return f"Command: {self.name}"


EvaluatedRule = typing.NewType("EvaluatedRule", PullRequestRule)


class QueueConfig(typing.TypedDict):
    priority: int
    speculative_checks: int
    batch_size: int
    batch_max_wait_time: datetime.timedelta
    allow_inplace_checks: bool
    disallow_checks_interruption_from_queues: typing.List[str]
    checks_timeout: typing.Optional[datetime.timedelta]
    draft_bot_account: typing.Optional[github_types.GitHubLogin]
    queue_branch_prefix: typing.Optional[str]


EvaluatedQueueRule = typing.NewType("EvaluatedQueueRule", "QueueRule")


QueueName = typing.NewType("QueueName", str)


@dataclasses.dataclass
class QueueRule:
    name: QueueName
    conditions: conditions_mod.QueueRuleConditions
    config: QueueConfig

    class T_from_dict(QueueConfig, total=False):
        name: QueueName
        conditions: conditions_mod.QueueRuleConditions

    @classmethod
    def from_dict(cls, d: T_from_dict) -> "QueueRule":
        name = d.pop("name")
        conditions = d.pop("conditions")

        # NOTE(sileht): backward compat
        allow_inplace_speculative_checks = d["allow_inplace_speculative_checks"]  # type: ignore[typeddict-item]
        if allow_inplace_speculative_checks is not None:
            d["allow_inplace_checks"] = allow_inplace_speculative_checks

        allow_checks_interruption = d["allow_checks_interruption"]  # type: ignore[typeddict-item]
        if allow_checks_interruption is None:
            allow_checks_interruption = d["allow_speculative_checks_interruption"]  # type: ignore[typeddict-item]

        # TODO(sileht): We should just delete this option at some point and hardcode the false behavior by default
        if allow_checks_interruption is False:
            d["disallow_checks_interruption_from_queues"].append(name)

        return cls(name, conditions, d)

    async def get_evaluated_queue_rule(
        self,
        repository: context.Repository,
        ref: github_types.GitHubRefType,
        pulls: typing.List[context.BasePullRequest],
    ) -> EvaluatedQueueRule:
        extra_conditions = await conditions_mod.get_branch_protection_conditions(
            repository, ref, strict=False
        )

        queue_rule_with_branch_protection = QueueRule(
            self.name,
            conditions_mod.QueueRuleConditions(
                extra_conditions + self.conditions.condition.copy().conditions
            ),
            self.config,
        )
        queue_rules_evaluator = await QueuesRulesEvaluator.create(
            [queue_rule_with_branch_protection],
            repository,
            pulls,
            False,
        )
        return queue_rules_evaluator.matching_rules[0]

    async def evaluate(
        self, pulls: typing.List[context.BasePullRequest]
    ) -> EvaluatedQueueRule:
        await self.conditions(pulls)
        return typing.cast(EvaluatedQueueRule, self)


T_Rule = typing.TypeVar("T_Rule", PullRequestRule, QueueRule)
T_EvaluatedRule = typing.TypeVar("T_EvaluatedRule", EvaluatedRule, EvaluatedQueueRule)


@dataclasses.dataclass
class GenericRulesEvaluator(typing.Generic[T_Rule, T_EvaluatedRule]):
    """A rules that matches a pull request."""

    # Fixed base attributes that are not considered when looking for the
    # next matching rules.
    BASE_ATTRIBUTES = (
        "head",
        "author",
        "merged_by",
    )

    # Base attributes that are looked for in the rules matching, but
    # should not appear in summary.
    # Note(Syffe): this list was created after GitHub integrated the possibility to change
    # the base branch of a PR. It invalidated the unchangeable state of this base attribute.
    # and thus we needed to process it separately from the other base attributes.
    BASE_CHANGEABLE_ATTRIBUTES = ("base",)

    # The list of pull request rules to match against.
    rules: typing.List[T_Rule]

    # The rules matching the pull request.
    matching_rules: typing.List[T_EvaluatedRule] = dataclasses.field(
        init=False, default_factory=list
    )

    # The rules that can't be computed due to runtime error (eg: team resolution failure)
    faulty_rules: typing.List[T_EvaluatedRule] = dataclasses.field(
        init=False, default_factory=list
    )

    # The rules not matching the pull request.
    ignored_rules: typing.List[T_EvaluatedRule] = dataclasses.field(
        init=False, default_factory=list
    )

    # The rules not matching the base changeable attributes
    not_applicable_base_changeable_attributes_rules: typing.List[
        T_EvaluatedRule
    ] = dataclasses.field(init=False, default_factory=list)

    @classmethod
    async def create(
        cls,
        rules: typing.List[T_Rule],
        repository: context.Repository,
        pulls: typing.List[context.BasePullRequest],
        rule_hidden_from_merge_queue: bool,
    ) -> "GenericRulesEvaluator[T_Rule, T_EvaluatedRule]":
        self = cls(rules)

        for rule in self.rules:
            for condition in rule.conditions.walk():
                live_resolvers.configure_filter(repository, condition.partial_filter)

            evaluated_rule = typing.cast(T_EvaluatedRule, await rule.evaluate(pulls))  # type: ignore[redundant-cast]
            del rule

            # NOTE(sileht):
            # In the summary, we display rules in five groups:
            # * rules where all attributes match -> matching_rules
            # * rules where only BASE_ATTRIBUTES match (filter out rule written for other branches) -> matching_rules
            # * rules that won't work due to a configuration issue detected at runtime (eg team doesn't exists) -> faulty_rules
            # * rules where only BASE_ATTRIBUTES don't match (mainly to hide rule written for other branches) -> ignored_rules
            # * rules where only BASE_CHANGEABLE ATTRIBUTES don't match (they rarely change but are handled)-> not_applicable_base_changeable_attributes_rules
            categorized = False
            if rule_hidden_from_merge_queue and not evaluated_rule.conditions.match:
                # NOTE(sileht): Replace non-base attribute and non-base changeables attributes
                # by true, if it still matches it's a potential rule otherwise hide it.
                base_changeable_conditions = evaluated_rule.conditions.copy()
                for condition in base_changeable_conditions.walk():
                    attr = condition.get_attribute_name()
                    if attr not in self.BASE_CHANGEABLE_ATTRIBUTES:
                        condition.update("number>0")

                base_conditions = evaluated_rule.conditions.copy()
                for condition in base_conditions.walk():
                    attr = condition.get_attribute_name()
                    if attr not in self.BASE_ATTRIBUTES:
                        condition.update("number>0")

                await base_changeable_conditions(pulls)
                await base_conditions(pulls)

                if not base_changeable_conditions.match:
                    self.not_applicable_base_changeable_attributes_rules.append(
                        evaluated_rule
                    )

                if not base_conditions.match:
                    self.ignored_rules.append(evaluated_rule)
                    categorized = True

                if not categorized and evaluated_rule.conditions.is_faulty():
                    self.faulty_rules.append(evaluated_rule)
                    categorized = True

            if not categorized:
                self.matching_rules.append(evaluated_rule)

        return self


RulesEvaluator = GenericRulesEvaluator[PullRequestRule, EvaluatedRule]
QueuesRulesEvaluator = GenericRulesEvaluator[QueueRule, EvaluatedQueueRule]


@dataclasses.dataclass
class PullRequestRules:
    rules: typing.List[PullRequestRule]

    def __post_init__(self) -> None:
        # NOTE(sileht): Make sure each rule has a unique name because they are
        # used to serialize the rule/action result in summary. And the summary
        # uses as unique key something like: f"{rule.name} ({action.name})"
        sorted_rules = sorted(self.rules, key=operator.attrgetter("name"))
        grouped_rules = itertools.groupby(sorted_rules, operator.attrgetter("name"))
        for _, sub_rules in grouped_rules:
            sub_rules_list = list(sub_rules)
            if len(sub_rules_list) == 1:
                continue
            for n, rule in enumerate(sub_rules_list):
                rule.name += f" #{n + 1}"

    def __iter__(self) -> typing.Iterator[PullRequestRule]:
        return iter(self.rules)

    def has_user_rules(self) -> bool:
        return any(rule for rule in self.rules if not rule.hidden)

    @staticmethod
    def _gen_rule_from(
        rule: PullRequestRule,
        new_actions: typing.Dict[str, actions_mod.Action],
        extra_conditions: typing.List[
            typing.Union[
                conditions_mod.RuleConditionGroup, conditions_mod.RuleCondition
            ]
        ],
    ) -> PullRequestRule:
        return PullRequestRule(
            name=rule.name,
            disabled=rule.disabled,
            conditions=conditions_mod.PullRequestRuleConditions(
                rule.conditions.condition.copy().conditions + extra_conditions
            ),
            actions=new_actions,
            hidden=rule.hidden,
        )

    async def get_pull_request_rule(self, ctxt: context.Context) -> RulesEvaluator:
        runtime_rules = []
        for rule in self.rules:
            if not rule.actions:
                runtime_rules.append(self._gen_rule_from(rule, rule.actions, []))
                continue

            actions_without_special_rules = {}
            for name, action in rule.actions.items():
                conditions = await action.get_conditions_requirements(ctxt)
                if conditions:
                    runtime_rules.append(
                        self._gen_rule_from(rule, {name: action}, conditions)
                    )
                else:
                    actions_without_special_rules[name] = action

            if actions_without_special_rules:
                runtime_rules.append(
                    self._gen_rule_from(rule, actions_without_special_rules, [])
                )

        return await RulesEvaluator.create(
            runtime_rules,
            ctxt.repository,
            [ctxt.pull_request],
            True,
        )


@dataclasses.dataclass
class QueueRules:
    rules: typing.List[QueueRule]

    def __iter__(self) -> typing.Iterator[QueueRule]:
        return iter(self.rules)

    def __getitem__(self, key: QueueName) -> QueueRule:
        for rule in self:
            if rule.name == key:
                return rule
        raise KeyError(f"{key} not found")

    def get(self, key: QueueName) -> typing.Optional[QueueRule]:
        try:
            return self[key]
        except KeyError:
            return None

    def __len__(self) -> int:
        return len(self.rules)

    def __post_init__(self) -> None:
        names: typing.Set[QueueName] = set()
        for i, rule in enumerate(reversed(self.rules)):
            rule.config["priority"] = i
            if rule.name in names:
                raise voluptuous.error.Invalid(
                    f"queue_rules names must be unique, found `{rule.name}` twice"
                )
            names.add(rule.name)

        for rule in self.rules:
            for name in rule.config["disallow_checks_interruption_from_queues"]:
                if name not in names:
                    raise voluptuous.error.Invalid(
                        f"disallow_checks_interruption_from_queues containes an unkown queue: {name}"
                    )


class YAMLInvalid(voluptuous.Invalid):  # type: ignore[misc]
    def __str__(self) -> str:
        return f"{self.msg} at {self.path}"

    def get_annotations(self, path: str) -> typing.List[github_types.GitHubAnnotation]:
        if self.path:
            error_path = self.path[0]
            return [
                {
                    "path": path,
                    "start_line": error_path.line,
                    "end_line": error_path.line,
                    "start_column": error_path.column,
                    "end_column": error_path.column,
                    "annotation_level": "failure",
                    "message": self.error_message,
                    "title": self.msg,
                },
            ]
        return []


@tracer.wrap("yaml.load")
def YAML(v: str) -> typing.Any:
    try:
        return yaml.safe_load(v)
    except yaml.MarkedYAMLError as e:
        error_message = str(e)
        path = []
        if e.problem_mark is not None:
            path.append(
                types.LineColumnPath(e.problem_mark.line + 1, e.problem_mark.column + 1)
            )
        raise YAMLInvalid(
            message="Invalid YAML", error_message=error_message, path=path
        )
    except yaml.YAMLError as e:
        error_message = str(e)
        raise YAMLInvalid(message="Invalid YAML", error_message=error_message)


def RuleConditionSchema(v: typing.Any, depth: int = 0) -> typing.Any:
    if depth > 8:
        raise voluptuous.Invalid("Maximun number of nested conditions reached")

    return voluptuous.Schema(
        voluptuous.Any(
            voluptuous.All(str, voluptuous.Coerce(conditions_mod.RuleCondition)),
            voluptuous.All(
                {
                    "and": voluptuous.All(
                        [lambda v: RuleConditionSchema(v, depth + 1)],
                        voluptuous.Length(min=2),
                    ),
                    "or": voluptuous.All(
                        [lambda v: RuleConditionSchema(v, depth + 1)],
                        voluptuous.Length(min=2),
                    ),
                },
                voluptuous.Length(min=1, max=1),
                voluptuous.Coerce(conditions_mod.RuleConditionGroup),
            ),
        )
    )(v)


def get_pull_request_rules_schema() -> voluptuous.All:
    return voluptuous.All(
        [
            voluptuous.All(
                {
                    voluptuous.Required("name"): str,
                    voluptuous.Required("disabled", default=None): voluptuous.Any(
                        None, {voluptuous.Required("reason"): str}
                    ),
                    voluptuous.Required("hidden", default=False): bool,
                    voluptuous.Required("conditions"): voluptuous.All(
                        [voluptuous.Coerce(RuleConditionSchema)],
                        voluptuous.Coerce(conditions_mod.PullRequestRuleConditions),
                    ),
                    voluptuous.Required("actions"): actions_mod.get_action_schemas(),
                },
                voluptuous.Coerce(PullRequestRule.from_dict),
            ),
        ],
        voluptuous.Coerce(PullRequestRules),
    )


def PositiveInterval(v: str) -> datetime.timedelta:
    try:
        td = date.interval_from_string(v)
    except date.InvalidDate as e:
        raise voluptuous.Invalid(e.message)

    if td < datetime.timedelta(seconds=0):
        raise voluptuous.Invalid("Interval must be positive")
    return td


def ChecksTimeout(v: str) -> datetime.timedelta:
    try:
        td = date.interval_from_string(v)
    except date.InvalidDate as e:
        raise voluptuous.Invalid(e.message)
    if td < datetime.timedelta(seconds=60):
        raise voluptuous.Invalid("Interval must be greater than 60 seconds")
    return td


QueueRulesSchema = voluptuous.All(
    [
        voluptuous.All(
            {
                voluptuous.Required("name"): str,
                voluptuous.Required("conditions"): voluptuous.All(
                    [voluptuous.Coerce(RuleConditionSchema)],
                    voluptuous.Coerce(conditions_mod.QueueRuleConditions),
                ),
                voluptuous.Required("speculative_checks", default=1): voluptuous.All(
                    int, voluptuous.Range(min=1, max=20)
                ),
                voluptuous.Required("batch_size", default=1): voluptuous.All(
                    int, voluptuous.Range(min=1, max=20)
                ),
                voluptuous.Required(
                    "batch_max_wait_time", default="30 s"
                ): voluptuous.All(str, voluptuous.Coerce(PositiveInterval)),
                voluptuous.Required("allow_inplace_checks", default=True): bool,
                voluptuous.Required(
                    "disallow_checks_interruption_from_queues", default=[]
                ): [str],
                voluptuous.Required("checks_timeout", default=None): voluptuous.Any(
                    None, voluptuous.All(str, voluptuous.Coerce(ChecksTimeout))
                ),
                voluptuous.Required("draft_bot_account", default=None): voluptuous.Any(
                    None, str
                ),
                voluptuous.Required(
                    "queue_branch_prefix",
                    default=constants.MERGE_QUEUE_BRANCH_PREFIX,
                ): str,
                # TODO(sileht): options to deprecate
                voluptuous.Required(
                    "allow_checks_interruption", default=None
                ): voluptuous.Any(None, bool),
                # Deprecated options
                voluptuous.Required(
                    "allow_inplace_speculative_checks", default=None
                ): voluptuous.Any(None, bool),
                voluptuous.Required(
                    "allow_speculative_checks_interruption", default=None
                ): voluptuous.Any(None, bool),
            },
            voluptuous.Coerce(QueueRule.from_dict),
        )
    ],
    voluptuous.Coerce(QueueRules),
)


def get_defaults_schema() -> typing.Dict[typing.Any, typing.Any]:
    return {
        # FIXME(sileht): actions.get_action_schemas() returns only actions Actions
        # and not command only, since only refresh is command only and it doesn't
        # have options it's not a big deal.
        voluptuous.Required("actions", default={}): actions_mod.get_action_schemas(),
    }


def FullifyPullRequestRules(v: "MergifyConfig") -> "MergifyConfig":
    try:
        for pr_rule in v["pull_request_rules"]:
            for action in pr_rule.actions.values():
                action.validate_config(v)
    except voluptuous.error.Error:
        raise
    except Exception as e:
        LOG.error("fail to dispatch config", exc_info=True)
        raise voluptuous.error.Invalid(str(e))
    return v


CommandsRestrictionsSchema = {
    voluptuous.Required("conditions", default=[]): voluptuous.All(
        [voluptuous.Coerce(RuleConditionSchema)],
        voluptuous.Coerce(conditions_mod.PullRequestRuleConditions),
    )
}


def UserConfigurationSchema(
    config: typing.Dict[str, typing.Any], partial_validation: bool = False
) -> voluptuous.Schema:
    schema = {
        voluptuous.Required(
            "pull_request_rules", default=[]
        ): get_pull_request_rules_schema(),
        voluptuous.Required(
            "queue_rules", default=[{"name": "default", "conditions": []}]
        ): QueueRulesSchema,
        voluptuous.Required("commands_restrictions", default={}): {
            voluptuous.Required(name, default={}): CommandsRestrictionsSchema
            for name in actions_mod.get_commands()
        },
        voluptuous.Required("defaults", default={}): get_defaults_schema(),
        voluptuous.Remove("shared"): voluptuous.Any(dict, list, str, int, float, bool),
    }

    if not partial_validation:
        schema = voluptuous.And(schema, voluptuous.Coerce(FullifyPullRequestRules))

    return voluptuous.Schema(schema)(config)


YamlSchema: typing.Callable[[str], typing.Any] = voluptuous.Schema(
    voluptuous.Coerce(YAML)
)


@dataclasses.dataclass
class InvalidRules(Exception):
    error: voluptuous.Invalid
    filename: str

    @staticmethod
    def _format_path_item(path_item: typing.Any) -> str:
        if isinstance(path_item, int):
            return f"item {path_item}"
        return str(path_item)

    @classmethod
    def format_error(cls, error: voluptuous.Invalid) -> str:
        msg = str(error.msg)

        if error.error_type:
            msg += f" for {error.error_type}"

        if error.path:
            path = " → ".join(map(cls._format_path_item, error.path))
            msg += f" @ {path}"
        # Only include the error message if it has been provided
        # voluptuous set it to the `message` otherwise
        if error.error_message != error.msg:
            msg += f"\n```\n{error.error_message}\n```"
        return msg

    @classmethod
    def _walk_error(
        cls, root_error: voluptuous.Invalid
    ) -> typing.Generator[voluptuous.Invalid, None, None]:
        if isinstance(root_error, voluptuous.MultipleInvalid):
            for error1 in root_error.errors:
                for error2 in cls._walk_error(error1):
                    yield error2
        else:
            yield root_error

    @property
    def errors(self) -> typing.List[voluptuous.Invalid]:
        return list(self._walk_error(self.error))

    def __str__(self) -> str:
        if len(self.errors) >= 2:
            return "* " + "\n* ".join(sorted(map(self.format_error, self.errors)))
        return self.format_error(self.errors[0])

    def get_annotations(self, path: str) -> typing.List[github_types.GitHubAnnotation]:
        return functools.reduce(
            operator.add,
            (
                error.get_annotations(path)
                for error in self.errors
                if hasattr(error, "get_annotations")
            ),
            [],
        )


class Defaults(typing.TypedDict):
    actions: typing.Dict[str, typing.Any]


class CommandsRestrictions(typing.TypedDict):
    conditions: conditions_mod.PullRequestRuleConditions


class MergifyConfig(typing.TypedDict):
    pull_request_rules: PullRequestRules
    queue_rules: QueueRules
    defaults: Defaults
    commands_restrictions: typing.Dict[str, CommandsRestrictions]
    raw_config: typing.Any


def merge_config(
    config: typing.Dict[str, typing.Any], defaults: typing.Dict[str, typing.Any]
) -> typing.Dict[str, typing.Any]:
    if defaults_actions := defaults.get("actions"):
        for rule in config.get("pull_request_rules", []):
            actions = rule["actions"]

            for action_name, action in actions.items():
                if action_name not in defaults_actions:
                    continue
                elif defaults_actions[action_name] is None:
                    continue

                if action is None:
                    rule["actions"][action_name] = defaults_actions[action_name]
                else:
                    merged_action = defaults_actions[action_name] | action
                    rule["actions"][action_name].update(merged_action)
    return config


def get_mergify_config(
    config_file: context.MergifyConfigFile,
) -> MergifyConfig:
    try:
        config = YamlSchema(config_file["decoded_content"])
    except voluptuous.Invalid as e:
        raise InvalidRules(e, config_file["path"])

    # Allow an empty file
    if config is None:
        config = {}

    # Validate defaults
    try:
        UserConfigurationSchema(config, partial_validation=True)
    except voluptuous.Invalid as e:
        raise InvalidRules(e, config_file["path"])

    defaults = config.pop("defaults", {})
    merged_config = merge_config(config, defaults)

    try:
        final_config = UserConfigurationSchema(merged_config, partial_validation=False)
        final_config["defaults"] = defaults
        final_config["raw_config"] = config
        return typing.cast(MergifyConfig, final_config)
    except voluptuous.Invalid as e:
        raise InvalidRules(e, config_file["path"])


MERGIFY_BUILTIN_CONFIG_YAML = f"""
pull_request_rules:
  - name: delete backport/copy branch (Mergify rule)
    hidden: true
    conditions:
      - author={config.BOT_USER_LOGIN}
      - head~=^mergify/(bp|copy)/
      - closed
    actions:
        delete_head_branch:
"""

MERGIFY_BUILTIN_CONFIG = UserConfigurationSchema(
    YamlSchema(MERGIFY_BUILTIN_CONFIG_YAML)
)
