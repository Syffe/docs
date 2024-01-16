from __future__ import annotations

import dataclasses
import datetime
import html
import typing

import typing_extensions
import voluptuous

from mergify_engine import condition_value_querier
from mergify_engine import constants
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import queue
from mergify_engine.actions import merge_base
from mergify_engine.queue import merge_train
from mergify_engine.rules import conditions as conditions_mod
from mergify_engine.rules import generic_evaluator
from mergify_engine.rules import types
from mergify_engine.rules.config import conditions as cond_config
from mergify_engine.rules.config import priority_rules as priority_rules_config
from mergify_engine.rules.config import pull_request_rules as pull_request_rules_config


if typing.TYPE_CHECKING:
    from collections import abc

    from mergify_engine import context

MAX_QUEUE_RULES = 50
MAX_PRIORITY_RULES = 50

QueueBranchMergeMethod = typing.Literal["fast-forward"] | None
QueueName = typing.NewType("QueueName", str)

EvaluatedQueueRule = typing.NewType("EvaluatedQueueRule", "QueueRule")

QueueRulesEvaluator = generic_evaluator.GenericRulesEvaluator[
    "QueueRule",
    EvaluatedQueueRule,
]


class QueueConfig(typing_extensions.TypedDict):
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
    merge_method: merge_base.MergeMethodT | None
    merge_bot_account: github_types.GitHubLogin | None
    update_method: typing.Literal["rebase", "merge"] | None
    update_bot_account: github_types.GitHubLogin | None
    autosquash: bool


@dataclasses.dataclass
class QueueRule:
    name: QueueName
    config: QueueConfig
    merge_conditions: conditions_mod.QueueRuleMergeConditions
    queue_conditions: conditions_mod.QueueRuleMergeConditions
    priority_rules: priority_rules_config.PriorityRules
    require_branch_protection: bool
    branch_protection_injection_mode: queue.BranchProtectionInjectionModeT

    class TypeFromDict(QueueConfig, total=False):
        name: QueueName
        config: QueueConfig
        # NOTE: conditions is the deprecated variant of merge_conditions
        conditions: conditions_mod.QueueRuleMergeConditions
        merge_conditions: conditions_mod.QueueRuleMergeConditions
        # NOTE: routing_conditions is the deprecated variant of queue_conditions
        routing_conditions: conditions_mod.QueueRuleMergeConditions
        queue_conditions: conditions_mod.QueueRuleMergeConditions
        priority_rules: priority_rules_config.PriorityRules
        require_branch_protection: bool
        branch_protection_injection_mode: queue.BranchProtectionInjectionModeT

    @classmethod
    def from_dict(cls, d: TypeFromDict) -> QueueRule:
        name = d.pop("name")
        # NOTE(jd): deprecated name, for backward compat
        merge_conditions = d.pop("conditions", None)
        if merge_conditions is None:
            merge_conditions = d.pop("merge_conditions")

        # NOTE: deprecated name, for backward compat
        queue_conditions = d.pop("routing_conditions", None)
        if queue_conditions is None:
            queue_conditions = d.pop("queue_conditions")

        branch_protection_injection_mode = d.pop(
            "branch_protection_injection_mode",
            "queue",
        )
        require_branch_protection = d.pop("require_branch_protection")

        priority_rules = d.pop("priority_rules")

        # NOTE(sileht): backward compat
        allow_inplace_speculative_checks = d["allow_inplace_speculative_checks"]  # type: ignore[typeddict-item]
        if allow_inplace_speculative_checks is not None:
            d["allow_inplace_checks"] = allow_inplace_speculative_checks

        allow_checks_interruption = d["allow_checks_interruption"]  # type: ignore[typeddict-item]
        if allow_checks_interruption is None:
            allow_checks_interruption = d["allow_speculative_checks_interruption"]  # type: ignore[typeddict-item]

        if allow_checks_interruption is False:
            d["disallow_checks_interruption_from_queues"].append(name)

        return cls(
            name=name,
            config=d,
            merge_conditions=merge_conditions,
            queue_conditions=queue_conditions,
            priority_rules=priority_rules,
            require_branch_protection=require_branch_protection,
            branch_protection_injection_mode=branch_protection_injection_mode,
        )

    async def get_queue_rule_for_evaluator(
        self,
        repository: context.Repository,
        ref: github_types.GitHubRefType,
        evaluated_pull_request_rule: pull_request_rules_config.EvaluatedPullRequestRule
        | None = None,
    ) -> QueueRule:
        branch_protections: list[conditions_mod.RuleConditionNode] = []
        if self.branch_protection_injection_mode != "none":
            branch_protections.extend(
                await conditions_mod.get_branch_protection_conditions(
                    repository,
                    ref,
                    strict=False,
                ),
            )

        pull_request_rules_conditions: list[conditions_mod.RuleConditionNode] = []
        if evaluated_pull_request_rule is not None:
            evaluated_pull_request_rule_conditions = (
                evaluated_pull_request_rule.conditions.copy()
            )
            if evaluated_pull_request_rule_conditions.condition.conditions:
                pull_request_rules_conditions.extend(
                    [
                        conditions_mod.RuleConditionCombination(
                            {
                                "and": evaluated_pull_request_rule_conditions.condition.conditions,
                            },
                            description=f"📃 From pull request rule **{html.escape(evaluated_pull_request_rule.name)}**",
                        ),
                    ],
                )

        merge_conditions = conditions_mod.QueueRuleMergeConditions(
            branch_protections
            + pull_request_rules_conditions
            + self.merge_conditions.condition.copy().conditions,
        )

        if self.branch_protection_injection_mode == "queue":
            queue_conditions = conditions_mod.QueueRuleMergeConditions(
                branch_protections
                + pull_request_rules_conditions
                + self.queue_conditions.condition.copy().conditions,
            )
        else:
            queue_conditions = conditions_mod.QueueRuleMergeConditions(
                pull_request_rules_conditions
                + self.queue_conditions.condition.copy().conditions,
            )

        return QueueRule(
            name=self.name,
            merge_conditions=merge_conditions,
            queue_conditions=queue_conditions,
            config=self.config,
            priority_rules=self.priority_rules,
            require_branch_protection=self.require_branch_protection,
            branch_protection_injection_mode=self.branch_protection_injection_mode,
        )

    async def get_evaluated_queue_rule(
        self,
        repository: context.Repository,
        ref: github_types.GitHubRefType,
        pulls: list[condition_value_querier.BasePullRequest],
        evaluated_pull_request_rule: pull_request_rules_config.EvaluatedPullRequestRule
        | None = None,
    ) -> EvaluatedQueueRule:
        queue_rule_with_extra_conditions = await self.get_queue_rule_for_evaluator(
            repository,
            ref,
            evaluated_pull_request_rule,
        )
        queue_rules_evaluator = await QueueRulesEvaluator.create(
            [queue_rule_with_extra_conditions],
            repository,
            pulls,
            rule_hidden_from_merge_queue=False,
        )
        return queue_rules_evaluator.matching_rules[0]

    def get_conditions_used_by_evaluator(
        self,
    ) -> conditions_mod.QueueRuleMergeConditions:
        return conditions_mod.QueueRuleMergeConditions(
            self.merge_conditions.condition.conditions
            + self.queue_conditions.condition.conditions,
        )

    async def evaluate(
        self,
        pulls: list[condition_value_querier.BasePullRequest],
    ) -> EvaluatedQueueRule:
        await self.merge_conditions(pulls)

        # NOTE(Greesb): We re-cast QueuePullRequest into simple PullRequest
        # because we want to evaluate the queue_conditions on
        # the queued pull requests, not the draft pr (if any).
        # The type ignore is because mypy thinks the pulls are `BasePullRequest`
        # and doesn't understand that in this `if` all the pull requests
        # in `pulls` are of `QueuePullRequest` instance.
        pulls = [
            condition_value_querier.PullRequest(
                typing.cast(condition_value_querier.QueuePullRequest, p).context,
            )
            for p in pulls
        ]

        await self.queue_conditions(pulls)
        return typing.cast(EvaluatedQueueRule, self)

    async def get_context_effective_priority(self, ctxt: context.Context) -> int:
        effective_priority = (
            await self.priority_rules.get_context_priority(ctxt)
            + self.config["priority"] * queue.QUEUE_PRIORITY_OFFSET
        )

        convoy = await merge_train.Convoy.from_context(ctxt)

        # NOTE(charly): if some dependent pull requests are already in the
        # queue, pick the lowest priority to preserve the dependency graph
        for dependent_pr_number in ctxt.get_depends_on():
            dependent_embarked_prs = await convoy.find_embarked_pull(
                dependent_pr_number,
            )
            for _, dependent_embarked_pr, _, _ in dependent_embarked_prs:
                dependent_effective_priority = dependent_embarked_pr.config[
                    "effective_priority"
                ]
                if dependent_effective_priority < effective_priority:
                    effective_priority = dependent_effective_priority

        return effective_priority


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
                    f"queue_rules names must be unique, found `{rule.name}` twice",
                )
            names.add(rule.name)

        for rule in self.rules:
            for name in rule.config["disallow_checks_interruption_from_queues"]:
                if name not in names:
                    raise voluptuous.error.Invalid(
                        f"disallow_checks_interruption_from_queues contains an unkown queue: {name}",
                    )

    @property
    def names(self) -> tuple[QueueName, ...]:
        return tuple(rule.name for rule in self.rules)

    async def queue_conditions_exists(self) -> bool:
        return any(rule.queue_conditions.condition.conditions for rule in self.rules)

    async def get_evaluated_queue_conditions(
        self,
        ctxt: context.Context,
    ) -> list[EvaluatedQueueRule]:
        # NOTE(Syffe): in order to only evaluate queue_conditions (and not basic conditions) before queuing the PR,
        # we create a list of temporary QueueRules that only contains queue_conditions for evaluation
        routing_rules = [
            QueueRule(
                name=rule.name,
                merge_conditions=conditions_mod.QueueRuleMergeConditions([]),
                queue_conditions=conditions_mod.QueueRuleMergeConditions(
                    rule.queue_conditions.condition.copy().conditions,
                ),
                config=rule.config,
                priority_rules=rule.priority_rules,
                require_branch_protection=rule.require_branch_protection,
                branch_protection_injection_mode=rule.branch_protection_injection_mode,
            )
            for rule in self.rules
        ]

        routing_rules_evaluator = await QueueRulesEvaluator.create(
            routing_rules,
            ctxt.repository,
            [condition_value_querier.PullRequest(ctxt)],
            rule_hidden_from_merge_queue=True,
        )
        return routing_rules_evaluator.matching_rules

    async def get_queue_rules_evaluator(
        self,
        ctxt: context.Context,
    ) -> QueueRulesEvaluator:
        runtime_rules = [
            QueueRule(
                name=rule.name,
                merge_conditions=conditions_mod.QueueRuleMergeConditions(
                    rule.merge_conditions.condition.copy().conditions,
                ),
                queue_conditions=conditions_mod.QueueRuleMergeConditions(
                    rule.queue_conditions.condition.copy().conditions,
                ),
                config=rule.config,
                priority_rules=rule.priority_rules,
                require_branch_protection=rule.require_branch_protection,
                branch_protection_injection_mode=rule.branch_protection_injection_mode,
            )
            for rule in self.rules
        ]

        return await QueueRulesEvaluator.create(
            runtime_rules,
            ctxt.repository,
            [condition_value_querier.PullRequest(ctxt)],
            rule_hidden_from_merge_queue=True,
        )


def PositiveInterval(v: str) -> datetime.timedelta:
    try:
        td = date.interval_from_string(v)
    except date.InvalidDateError as e:
        raise voluptuous.Invalid(e.message)

    if td < datetime.timedelta(seconds=0):
        raise voluptuous.Invalid("Interval must be positive")
    return td


def ChecksTimeout(v: str) -> datetime.timedelta:
    try:
        td = date.interval_from_string(v)
    except date.InvalidDateError as e:
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
        if not isinstance(obj, dict):
            raise RuntimeError(  # noqa: TRY004
                "_has_only_one_of() must be used after `obj` format has been valided by voluptuous",
            )

        if len(keys_set & obj.keys()) > 1:
            if msg is None:
                msg = f"Must contain only one of {','.join(keys)}"
            raise voluptuous.Invalid(msg)

        return obj

    return func


merge_conditions_exclusive_msg = "Cannot have both `conditions` and `merge_conditions`, only use `merge_conditions (`conditions` is deprecated)"
queue_conditions_exclusive_msg = "Cannot have both `routing_conditions` and `queue_conditions`, only use `queue_conditions (`routing_conditions` is deprecated)"

QueueRulesSchema = voluptuous.All(
    types.ListOf(
        voluptuous.All(
            {voluptuous.Extra: object},  # just ensure first it's a dict
            _has_only_one_of(
                "conditions",
                "merge_conditions",
                msg=merge_conditions_exclusive_msg,
            ),
            _has_only_one_of(
                "routing_conditions",
                "queue_conditions",
                msg=queue_conditions_exclusive_msg,
            ),
            {
                voluptuous.Required("name"): str,
                voluptuous.Required("priority_rules", default=list): voluptuous.All(
                    types.ListOf(
                        voluptuous.All(
                            {
                                voluptuous.Required("name"): str,
                                voluptuous.Required("conditions"): voluptuous.All(
                                    cond_config.ListOfRuleCondition(),
                                    voluptuous.Coerce(
                                        conditions_mod.PriorityRuleConditions,
                                    ),
                                ),
                                voluptuous.Required(
                                    "priority",
                                    default=queue.PriorityAliases.medium.value,
                                ): queue.PrioritySchema,
                            },
                            voluptuous.Coerce(
                                priority_rules_config.PriorityRule.from_dict,
                            ),
                        ),
                        MAX_PRIORITY_RULES,
                    ),
                    voluptuous.Coerce(priority_rules_config.PriorityRules),
                ),
                voluptuous.Required("merge_conditions", default=list): voluptuous.All(
                    cond_config.ListOfRuleCondition(),
                    voluptuous.Coerce(conditions_mod.QueueRuleMergeConditions),
                ),
                "conditions": voluptuous.All(
                    cond_config.ListOfRuleCondition(),
                    voluptuous.Coerce(conditions_mod.QueueRuleMergeConditions),
                ),
                voluptuous.Required("queue_conditions", default=list): voluptuous.All(
                    cond_config.ListOfRuleCondition(),
                    voluptuous.Coerce(conditions_mod.QueueRuleMergeConditions),
                ),
                "routing_conditions": voluptuous.All(
                    cond_config.ListOfRuleCondition(),
                    voluptuous.Coerce(conditions_mod.QueueRuleMergeConditions),
                ),
                voluptuous.Required("require_branch_protection", default=True): bool,
                voluptuous.Required(
                    "branch_protection_injection_mode",
                    default="queue",
                ): voluptuous.Any("queue", "merge", "none"),
                voluptuous.Required("speculative_checks", default=1): voluptuous.All(
                    int,
                    voluptuous.Range(min=1, max=128),
                ),
                voluptuous.Required("batch_size", default=1): voluptuous.All(
                    int,
                    voluptuous.Range(min=1, max=128),
                ),
                voluptuous.Required(
                    "batch_max_wait_time",
                    default="30 s",
                ): voluptuous.All(str, voluptuous.Coerce(PositiveInterval)),
                voluptuous.Required("allow_inplace_checks", default=True): bool,
                voluptuous.Required(
                    "disallow_checks_interruption_from_queues",
                    default=[],
                ): types.ListOf(str, _max=MAX_QUEUE_RULES),
                voluptuous.Required("checks_timeout", default=None): voluptuous.Any(
                    None,
                    voluptuous.All(str, voluptuous.Coerce(ChecksTimeout)),
                ),
                voluptuous.Required("draft_bot_account", default=None): voluptuous.Any(
                    None,
                    str,
                ),
                voluptuous.Required(
                    "queue_branch_merge_method",
                    default=None,
                ): voluptuous.Any(
                    None,
                    *QueueBranchMergeMethod.__args__[0].__args__,  # type: ignore[attr-defined]
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
                    "batch_max_failure_resolution_attempts",
                    default=None,
                ): voluptuous.Any(None, int),
                # Queue action options
                # NOTE(greesb): If you change the default values below, you might
                # need to change the retrocompatibility behavior in
                # mergify_engine/actions/queue.py::QueueExecutor.create
                voluptuous.Required(
                    "commit_message_template",
                    default=None,
                ): types.Jinja2WithNone,
                voluptuous.Required("merge_method", default=None): voluptuous.Any(
                    None,
                    *typing.get_args(merge_base.MergeMethodT),
                ),
                voluptuous.Required(
                    "merge_bot_account",
                    default=None,
                ): types.Jinja2WithNone,
                voluptuous.Required(
                    "update_bot_account",
                    default=None,
                ): types.Jinja2WithNone,
                voluptuous.Required("update_method", default=None): voluptuous.Any(
                    "rebase",
                    "merge",
                    None,
                ),
                voluptuous.Required("autosquash", default=True): bool,
                # TODO(sileht): options to deprecate
                voluptuous.Required(
                    "allow_checks_interruption",
                    default=None,
                ): voluptuous.Any(None, bool),
                # Deprecated options
                voluptuous.Required(
                    "allow_inplace_speculative_checks",
                    default=None,
                ): voluptuous.Any(None, bool),
                voluptuous.Required(
                    "allow_speculative_checks_interruption",
                    default=None,
                ): voluptuous.Any(None, bool),
            },
            voluptuous.Coerce(QueueRule.from_dict),
        ),
        MAX_QUEUE_RULES,
    ),
    voluptuous.Coerce(QueueRules),
)
