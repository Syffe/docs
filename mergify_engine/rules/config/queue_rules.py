from collections import abc
import dataclasses
import datetime
import html
import typing

import voluptuous

from mergify_engine import constants
from mergify_engine import context
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import queue
from mergify_engine import rules as base_rules
from mergify_engine.actions import merge_base
from mergify_engine.rules import conditions as conditions_mod
from mergify_engine.rules import types
from mergify_engine.rules.config import conditions as cond_config
from mergify_engine.rules.config import priority_rules as priority_rules_config
from mergify_engine.rules.config import pull_request_rules as pull_request_rules_config


QueueBranchMergeMethod = typing.Literal["fast-forward"] | None
QueueName = typing.NewType("QueueName", str)

EvaluatedQueueRule = typing.NewType("EvaluatedQueueRule", "QueueRule")

QueuesRulesEvaluator = base_rules.GenericRulesEvaluator["QueueRule", EvaluatedQueueRule]


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
    batch_max_failure_resolution_attempts: int | None
    # queue action config
    commit_message_template: str | None
    merge_method: merge_base.MergeMethodT
    merge_bot_account: github_types.GitHubLogin | None
    update_method: typing.Literal["rebase", "merge"] | None
    update_bot_account: github_types.GitHubLogin | None
    autosquash: bool


@dataclasses.dataclass
class QueueRule:
    name: QueueName
    config: QueueConfig
    merge_conditions: conditions_mod.QueueRuleMergeConditions
    routing_conditions: conditions_mod.QueueRuleMergeConditions
    priority_rules: priority_rules_config.PriorityRules

    class T_from_dict(QueueConfig, total=False):
        name: QueueName
        config: QueueConfig
        conditions: conditions_mod.QueueRuleMergeConditions
        merge_conditions: conditions_mod.QueueRuleMergeConditions
        routing_conditions: conditions_mod.QueueRuleMergeConditions
        priority_rules: priority_rules_config.PriorityRules

    @property
    def conditions(self) -> conditions_mod.QueueRuleMergeConditions:
        # NOTE(Greesb): This is needed by function using rules with
        # the typing "T_Rule"
        return self.merge_conditions

    @classmethod
    def from_dict(cls, d: T_from_dict) -> "QueueRule":
        name = d.pop("name")
        # NOTE(jd): deprecated name, for backward compat
        merge_conditions = d.pop("conditions", None)
        if merge_conditions is None:
            merge_conditions = d.pop("merge_conditions")
        routing_conditions = d.pop("routing_conditions")
        priority_rules = d.pop("priority_rules")

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

        return cls(
            name=name,
            config=d,
            merge_conditions=merge_conditions,
            routing_conditions=routing_conditions,
            priority_rules=priority_rules,
        )

    async def get_evaluated_queue_rule(
        self,
        repository: context.Repository,
        ref: github_types.GitHubRefType,
        pulls: list[context.BasePullRequest],
        evaluated_pull_request_rule: pull_request_rules_config.EvaluatedPullRequestRule
        | None = None,
    ) -> EvaluatedQueueRule:
        extra_conditions = await conditions_mod.get_branch_protection_conditions(
            repository, ref, strict=False
        )
        if evaluated_pull_request_rule is not None:
            evaluated_pull_request_rule_conditions = (
                evaluated_pull_request_rule.conditions.copy()
            )
            if evaluated_pull_request_rule_conditions.condition.conditions:
                extra_conditions.extend(
                    [
                        conditions_mod.RuleConditionCombination(
                            {
                                "and": evaluated_pull_request_rule_conditions.condition.conditions
                            },
                            description=f"📃 From pull request rule **{html.escape(evaluated_pull_request_rule.name)}**",
                        )
                    ]
                )

        queue_rule_with_extra_conditions = QueueRule(
            name=self.name,
            merge_conditions=conditions_mod.QueueRuleMergeConditions(
                extra_conditions + self.merge_conditions.condition.copy().conditions
            ),
            routing_conditions=conditions_mod.QueueRuleMergeConditions(
                self.routing_conditions.condition.copy().conditions
            ),
            config=self.config,
            priority_rules=self.priority_rules,
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
        await self.merge_conditions(pulls)
        await self.routing_conditions(pulls)
        return typing.cast(EvaluatedQueueRule, self)

    async def get_effective_priority(
        self, ctxt: context.Context, fallback_priority: int
    ) -> int:
        priority = await self.priority_rules.get_context_priority(ctxt)
        if priority is None:
            priority = fallback_priority

        return priority + self.config["priority"] * queue.QUEUE_PRIORITY_OFFSET


@dataclasses.dataclass
class QueueRules:
    rules: list[QueueRule]

    def __iter__(self) -> abc.Iterator[QueueRule]:
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
                        f"disallow_checks_interruption_from_queues contains an unkown queue: {name}"
                    )

    async def routing_conditions_exists(self) -> bool:
        for rule in self.rules:
            if rule.routing_conditions.condition.conditions:
                return True
        return False

    async def are_routing_conditions_matching(self, ctxt: context.Context) -> bool:
        if not await self.routing_conditions_exists():
            return False
        evaluated_routing_conditions = (
            await self.get_matching_evaluated_routing_conditions(ctxt=ctxt)
        )
        return bool(evaluated_routing_conditions)

    async def get_matching_evaluated_routing_conditions(
        self, ctxt: context.Context
    ) -> list[EvaluatedQueueRule]:
        # NOTE(Syffe): in order to only evaluate routing_conditions (and not basic conditions) before queuing the PR,
        # we create a list of temporary QueueRules that only contains routing_conditions for evaluation
        routing_rules = [
            QueueRule(
                name=rule.name,
                merge_conditions=conditions_mod.QueueRuleMergeConditions([]),
                routing_conditions=conditions_mod.QueueRuleMergeConditions(
                    rule.routing_conditions.condition.copy().conditions
                ),
                config=rule.config,
                priority_rules=rule.priority_rules,
            )
            for rule in self.rules
        ]

        routing_rules_evaluator = await QueuesRulesEvaluator.create(
            routing_rules,
            ctxt.repository,
            [ctxt.pull_request],
            True,
        )

        return [
            rule
            for rule in routing_rules_evaluator.matching_rules
            if rule.routing_conditions.match
        ]


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


def _has_only_one_of(
    *keys: str,
    msg: str | None = None,
) -> abc.Callable[[dict[str, typing.Any]], dict[str, typing.Any]]:
    keys_set = set(keys)

    if len(keys_set) < 2:
        raise ValueError("Need at least 2 keys to check for exclusivity")

    def func(obj: dict[str, typing.Any]) -> dict[str, typing.Any]:
        nonlocal msg
        if len(keys_set & obj.keys()) > 1:
            if msg is None:
                msg = f"Must contain only one of {','.join(keys)}"
            raise voluptuous.Invalid(msg)

        return obj

    return func


merge_conditions_exclusive_msg = "Cannot have both `conditions` and `merge_conditions`, only use `merge_conditions (`conditions` is deprecated)"

QueueRulesSchema = voluptuous.All(
    [
        voluptuous.All(
            _has_only_one_of(
                "conditions",
                "merge_conditions",
                msg=merge_conditions_exclusive_msg,
            ),
            {
                voluptuous.Required("name"): str,
                voluptuous.Required("priority_rules", default=list): voluptuous.All(
                    [
                        voluptuous.All(
                            {
                                voluptuous.Required("name"): str,
                                voluptuous.Required("conditions"): voluptuous.All(
                                    [
                                        voluptuous.Coerce(
                                            cond_config.RuleConditionSchema
                                        )
                                    ],
                                    voluptuous.Coerce(
                                        conditions_mod.PriorityRuleConditions
                                    ),
                                ),
                                voluptuous.Required(
                                    "priority",
                                    default=queue.PriorityAliases.medium.value,
                                ): queue.PrioritySchema,
                            },
                            voluptuous.Coerce(
                                priority_rules_config.PriorityRule.from_dict
                            ),
                        )
                    ],
                    voluptuous.Coerce(priority_rules_config.PriorityRules),
                ),
                voluptuous.Required("merge_conditions", default=list): voluptuous.All(
                    [voluptuous.Coerce(cond_config.RuleConditionSchema)],
                    voluptuous.Coerce(conditions_mod.QueueRuleMergeConditions),
                ),
                "conditions": voluptuous.All(
                    [voluptuous.Coerce(cond_config.RuleConditionSchema)],
                    voluptuous.Coerce(conditions_mod.QueueRuleMergeConditions),
                ),
                voluptuous.Required("routing_conditions", default=list): voluptuous.All(
                    [voluptuous.Coerce(cond_config.RuleConditionSchema)],
                    voluptuous.Coerce(conditions_mod.QueueRuleMergeConditions),
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
                    # NOTE(Greesb): Use a lambda to be able to mock the returned value,
                    # otherwise when the module is loaded the value is definitive and
                    # can't be mocked easily.
                    default=lambda: constants.MERGE_QUEUE_BRANCH_PREFIX,
                ): str,
                voluptuous.Required("allow_queue_branch_edit", default=False): bool,
                voluptuous.Required(
                    "batch_max_failure_resolution_attempts", default=None
                ): voluptuous.Any(None, int),
                # Queue action options
                # NOTE(greesb): If you change the default values below, you might
                # need to change the retrocompatibility behavior in
                # mergify_engine/actions/queue.py::QueueAction.validate_config
                voluptuous.Required(
                    "commit_message_template", default=None
                ): types.Jinja2WithNone,
                voluptuous.Required("merge_method", default="merge"): voluptuous.Any(
                    "rebase",
                    "merge",
                    "squash",
                    "fast-forward",
                ),
                voluptuous.Required(
                    "merge_bot_account", default=None
                ): types.Jinja2WithNone,
                voluptuous.Required(
                    "update_bot_account", default=None
                ): types.Jinja2WithNone,
                voluptuous.Required("update_method", default=None): voluptuous.Any(
                    "rebase", "merge", None
                ),
                voluptuous.Required("autosquash", default=True): bool,
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
