import dataclasses
import datetime
import functools
import html
import itertools
import operator
import typing

import daiquiri
from ddtrace import tracer
import voluptuous

from mergify_engine import actions as actions_mod
from mergify_engine import config
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import yaml
from mergify_engine.clients import http
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
    disabled: DisabledDict | None
    conditions: conditions_mod.PullRequestRuleConditions
    actions: dict[str, actions_mod.Action]
    hidden: bool = False
    from_command: bool = False

    class T_from_dict_required(typing.TypedDict):
        name: str
        disabled: DisabledDict | None
        conditions: conditions_mod.PullRequestRuleConditions
        actions: dict[str, actions_mod.Action]

    class T_from_dict(T_from_dict_required, total=False):
        hidden: bool

    @classmethod
    def from_dict(cls, d: T_from_dict) -> "PullRequestRule":
        return cls(**d)

    def get_check_name(self, action: str) -> str:
        return f"Rule: {self.name} ({action})"

    def get_signal_trigger(self) -> str:
        return f"Rule: {self.name}"

    async def evaluate(self, pulls: list[context.BasePullRequest]) -> "EvaluatedRule":
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


QueueBranchMergeMethod = typing.Optional[typing.Literal["fast-forward"]]


class QueueConfig(typing.TypedDict):
    priority: int
    speculative_checks: int
    batch_size: int
    batch_max_wait_time: datetime.timedelta
    allow_inplace_checks: bool
    disallow_checks_interruption_from_queues: list[str]
    checks_timeout: datetime.timedelta | None
    draft_bot_account: github_types.GitHubLogin | None
    queue_branch_prefix: str | None
    queue_branch_merge_method: QueueBranchMergeMethod
    allow_queue_branch_edit: bool


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
        pulls: list[context.BasePullRequest],
        evaluated_pull_request_rule: typing.Optional["EvaluatedRule"] = None,
    ) -> EvaluatedQueueRule:
        extra_conditions = await conditions_mod.get_branch_protection_conditions(
            repository, ref, strict=False
        )
        if evaluated_pull_request_rule is not None:
            conditions = evaluated_pull_request_rule.conditions.copy()
            if conditions.condition.conditions:
                extra_conditions.extend(
                    [
                        conditions_mod.RuleConditionCombination(
                            {"and": conditions.condition.conditions},
                            description=f"📃 From pull request rule **{html.escape(evaluated_pull_request_rule.name)}**",
                        )
                    ]
                )

        queue_rule_with_extra_conditions = QueueRule(
            self.name,
            conditions_mod.QueueRuleConditions(
                extra_conditions + self.conditions.condition.copy().conditions
            ),
            self.config,
        )
        queue_rules_evaluator = await QueuesRulesEvaluator.create(
            [queue_rule_with_extra_conditions],
            repository,
            pulls,
            False,
        )
        return queue_rules_evaluator.matching_rules[0]

    async def evaluate(
        self, pulls: list[context.BasePullRequest]
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
    rules: list[T_Rule]

    # The rules matching the pull request.
    matching_rules: list[T_EvaluatedRule] = dataclasses.field(
        init=False, default_factory=list
    )

    # The rules that can't be computed due to runtime error (eg: team resolution failure)
    faulty_rules: list[T_EvaluatedRule] = dataclasses.field(
        init=False, default_factory=list
    )

    # The rules not matching the pull request.
    ignored_rules: list[T_EvaluatedRule] = dataclasses.field(
        init=False, default_factory=list
    )

    # The rules not matching the base changeable attributes
    not_applicable_base_changeable_attributes_rules: list[
        T_EvaluatedRule
    ] = dataclasses.field(init=False, default_factory=list)

    @classmethod
    async def create(
        cls,
        rules: list[T_Rule],
        repository: context.Repository,
        pulls: list[context.BasePullRequest],
        rule_hidden_from_merge_queue: bool,
    ) -> "GenericRulesEvaluator[T_Rule, T_EvaluatedRule]":
        self = cls(rules)

        for rule in self.rules:
            apply_configure_filter(repository, rule.conditions)
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
    rules: list[PullRequestRule]
    has_multiple_rules_with_same_name: bool = False

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
                self.has_multiple_rules_with_same_name = True

    def __iter__(self) -> typing.Iterator[PullRequestRule]:
        return iter(self.rules)

    def has_user_rules(self) -> bool:
        return any(rule for rule in self.rules if not rule.hidden)

    @staticmethod
    def _gen_rule_from(
        rule: PullRequestRule,
        new_actions: dict[str, actions_mod.Action],
        extra_conditions: list[conditions_mod.RuleConditionNode],
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
    rules: list[QueueRule]

    def __iter__(self) -> typing.Iterator[QueueRule]:
        return iter(self.rules)

    def __getitem__(self, key: QueueName) -> QueueRule:
        for rule in self:
            if rule.name == key:
                return rule
        raise KeyError(f"{key} not found")

    def get(self, key: QueueName) -> QueueRule | None:
        try:
            return self[key]
        except KeyError:
            return None

    def __len__(self) -> int:
        return len(self.rules)

    def __post_init__(self) -> None:
        names: set[QueueName] = set()
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

    def get_annotations(self, path: str) -> list[github_types.GitHubAnnotation]:
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


def RuleConditionSchema(
    v: typing.Any, depth: int = 0, allow_command_attributes: bool = False
) -> typing.Any:
    if depth > 8:
        raise voluptuous.Invalid("Maximun number of nested conditions reached")

    return voluptuous.Schema(
        voluptuous.Any(
            voluptuous.All(
                str,
                voluptuous.Coerce(
                    lambda v: conditions_mod.RuleCondition(
                        v, allow_command_attributes=allow_command_attributes
                    )
                ),
            ),
            voluptuous.All(
                {
                    "and": voluptuous.All(
                        [
                            lambda v: RuleConditionSchema(
                                v,
                                depth + 1,
                                allow_command_attributes=allow_command_attributes,
                            )
                        ],
                        voluptuous.Length(min=2),
                    ),
                    "or": voluptuous.All(
                        [
                            lambda v: RuleConditionSchema(
                                v,
                                depth + 1,
                                allow_command_attributes=allow_command_attributes,
                            )
                        ],
                        voluptuous.Length(min=2),
                    ),
                },
                voluptuous.Length(min=1, max=1),
                voluptuous.Coerce(conditions_mod.RuleConditionCombination),
            ),
            voluptuous.All(
                {
                    "not": voluptuous.All(
                        voluptuous.Coerce(
                            lambda v: RuleConditionSchema(
                                v, depth + 1, allow_command_attributes
                            )
                        ),
                    ),
                },
                voluptuous.Length(min=1, max=1),
                voluptuous.Coerce(conditions_mod.RuleConditionNegation),
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
                    "queue_branch_merge_method", default=None
                ): voluptuous.Any(
                    None, *QueueBranchMergeMethod.__args__[0].__args__  # type: ignore[attr-defined]
                ),
                voluptuous.Required(
                    "queue_branch_prefix",
                    default=constants.MERGE_QUEUE_BRANCH_PREFIX,
                ): str,
                voluptuous.Required("allow_queue_branch_edit", default=False): bool,
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


def get_defaults_schema() -> dict[typing.Any, typing.Any]:
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


def CommandsRestrictionsSchema(
    command: type[actions_mod.Action],
) -> voluptuous.Schema:
    return {
        voluptuous.Required(
            "conditions", default=command.default_restrictions
        ): voluptuous.All(
            [
                voluptuous.Coerce(
                    lambda v: RuleConditionSchema(v, allow_command_attributes=True)
                )
            ],
            voluptuous.Coerce(conditions_mod.PullRequestRuleConditions),
        )
    }


def UserConfigurationSchema(
    config: dict[str, typing.Any], partial_validation: bool = False
) -> voluptuous.Schema:
    schema = {
        voluptuous.Required("extends", default=None): voluptuous.Any(
            None,
            voluptuous.All(
                str,
                voluptuous.Length(min=1),
            ),
        ),
        voluptuous.Required(
            "pull_request_rules", default=[]
        ): get_pull_request_rules_schema(),
        voluptuous.Required(
            "queue_rules", default=[{"name": "default", "conditions": []}]
        ): QueueRulesSchema,
        voluptuous.Required("commands_restrictions", default={}): {
            voluptuous.Required(name, default={}): CommandsRestrictionsSchema(command)
            for name, command in actions_mod.get_commands().items()
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
                yield from cls._walk_error(error1)
        else:
            yield root_error

    @property
    def errors(self) -> list[voluptuous.Invalid]:
        return list(self._walk_error(self.error))

    def __str__(self) -> str:
        if len(self.errors) >= 2:
            return "* " + "\n* ".join(sorted(map(self.format_error, self.errors)))
        return self.format_error(self.errors[0])

    def get_annotations(self, path: str) -> list[github_types.GitHubAnnotation]:
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
    actions: dict[str, typing.Any]


class CommandsRestrictions(typing.TypedDict):
    conditions: conditions_mod.PullRequestRuleConditions


class MergifyConfig(typing.TypedDict):
    extends: github_types.GitHubRepositoryName | None
    pull_request_rules: PullRequestRules
    queue_rules: QueueRules
    commands_restrictions: dict[str, CommandsRestrictions]
    defaults: Defaults
    raw_config: typing.Any


def merge_defaults(extended_defaults: Defaults, dest_defaults: Defaults) -> None:
    for action_name, action in extended_defaults.get("actions", {}).items():
        dest_actions = dest_defaults.setdefault("actions", {})
        dest_action = dest_actions.setdefault(action_name, {})
        for effect_name, effect_value in action.items():
            dest_action.setdefault(effect_name, effect_value)


def merge_config_with_defaults(
    config: dict[str, typing.Any], defaults: Defaults
) -> None:
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


def merge_raw_configs(
    extended_config: dict[str, typing.Any],
    dest_config: dict[str, typing.Any],
) -> None:
    for rule_to_merge in ("pull_request_rules", "queue_rules"):
        dest_rules = dest_config.setdefault(rule_to_merge, [])
        dest_rule_names = [rule["name"] for rule in dest_rules]

        for source_rule in extended_config.get(rule_to_merge, []):
            if source_rule["name"] not in dest_rule_names:
                dest_rules.append(source_rule)

    for commands_restriction in extended_config.get("commands_restrictions", {}):
        dest_config["commands_restrictions"].setdefault(
            commands_restriction,
            extended_config["commands_restrictions"][commands_restriction],
        )


async def get_mergify_config_from_file(
    repository_ctxt: context.Repository,
    config_file: context.MergifyConfigFile,
    allow_extend: bool = True,
) -> MergifyConfig:
    try:
        config = YamlSchema(config_file["decoded_content"])
    except voluptuous.Invalid as e:
        raise InvalidRules(e, config_file["path"])

    # Allow an empty file
    if config is None:
        config = {}

    # Validate defaults
    return await get_mergify_config_from_dict(
        repository_ctxt, config, config_file["path"], allow_extend
    )


async def get_mergify_config_from_dict(
    repository_ctxt: context.Repository,
    config: dict[str, typing.Any],
    error_path: str,
    allow_extend: bool = True,
) -> MergifyConfig:
    try:
        UserConfigurationSchema(config, partial_validation=True)
    except voluptuous.Invalid as e:
        raise InvalidRules(e, error_path)

    defaults = config.pop("defaults", {})

    extended_path = config.get("extends")
    if extended_path is not None:
        if not allow_extend:
            raise InvalidRules(
                voluptuous.Invalid(
                    "Maximum number of extended configuration reached. Limit is 1.",
                    ["extends"],
                ),
                error_path,
            )
        config_to_extend = await get_mergify_extended_config(
            repository_ctxt, extended_path, error_path
        )
        # NOTE(jules): Anchor and shared elements can't be shared between files
        # because they are computed by YamlSchema already.
        merge_defaults(config_to_extend["defaults"], defaults)
        merge_raw_configs(config_to_extend["raw_config"], config)

    merge_config_with_defaults(config, defaults)

    try:
        final_config = UserConfigurationSchema(config, partial_validation=False)
        final_config["defaults"] = defaults
        final_config["raw_config"] = config
        return typing.cast(MergifyConfig, final_config)
    except voluptuous.Invalid as e:
        raise InvalidRules(e, error_path)


async def get_mergify_extended_config(
    repository_ctxt: context.Repository,
    extended_path: github_types.GitHubRepositoryName,
    error_path: str,
) -> MergifyConfig:

    try:
        extended_repository_ctxt = (
            await repository_ctxt.installation.get_repository_by_name(extended_path)
        )
    except http.HTTPNotFound as e:
        exc = InvalidRules(
            voluptuous.Invalid(
                f"Extended configuration repository `{extended_path}` was not found. This repository doesn't exist or Mergify is not installed on it.",
                ["extends"],
                str(e),
            ),
            error_path,
        )
        raise exc from e

    if extended_repository_ctxt.repo["id"] == repository_ctxt.repo["id"]:
        raise InvalidRules(
            voluptuous.Invalid(
                "Only configuration from other repositories can be extended.",
                ["extends"],
            ),
            error_path,
        )

    return await extended_repository_ctxt.get_mergify_config(allow_extend=False)


def apply_configure_filter(
    repository: "context.Repository",
    conditions: (
        conditions_mod.PullRequestRuleConditions | conditions_mod.QueueRuleConditions
    ),
) -> None:
    for condition in conditions.walk():
        live_resolvers.configure_filter(repository, condition.partial_filter)


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
