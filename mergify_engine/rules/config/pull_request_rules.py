from collections import abc
import dataclasses
import typing

import daiquiri
import voluptuous

from mergify_engine import actions as actions_mod
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine.rules import conditions as conditions_mod
from mergify_engine.rules.config import conditions as cond_config


if typing.TYPE_CHECKING:
    from mergify_engine import context
    from mergify_engine.rules.config import mergify as mergify_conf


LOG = daiquiri.getLogger(__name__)

PullRequestRuleName = typing.NewType("PullRequestRuleName", str)
EvaluatedPullRequestRule = typing.NewType("EvaluatedPullRequestRule", "PullRequestRule")
PullRequestRulesEvaluator = rules.GenericRulesEvaluator[
    "PullRequestRule", EvaluatedPullRequestRule
]


@dataclasses.dataclass
class InvalidPullRequestRule(Exception):
    reason: str
    details: str


class DisabledDict(typing.TypedDict):
    reason: str


@dataclasses.dataclass
class PullRequestRule:
    name: PullRequestRuleName
    disabled: DisabledDict | None
    conditions: conditions_mod.PullRequestRuleConditions
    actions: dict[str, actions_mod.Action]
    hidden: bool

    class T_from_dict_required(typing.TypedDict):
        name: PullRequestRuleName
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

    async def evaluate(
        self, pulls: list["context.BasePullRequest"]
    ) -> EvaluatedPullRequestRule:
        evaluated_rule = typing.cast(EvaluatedPullRequestRule, self)
        await evaluated_rule.conditions(pulls)
        for action in self.actions.values():
            await action.load_context(
                typing.cast("context.PullRequest", pulls[0]).context, evaluated_rule
            )
        return evaluated_rule


@dataclasses.dataclass
class CommandRule(PullRequestRule):
    sender: github_types.GitHubAccount

    def get_signal_trigger(self) -> str:
        return f"Command: {self.name}"


@dataclasses.dataclass
class PullRequestRules:
    rules: list[PullRequestRule]

    def __post_init__(self) -> None:
        names: set[PullRequestRuleName] = set()
        for rule in self.rules:
            if rule.name in names:
                raise voluptuous.error.Invalid(
                    f"pull_request_rules names must be unique, found `{rule.name}` twice"
                )
            names.add(rule.name)

    def __iter__(self) -> abc.Iterator[PullRequestRule]:
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

    async def get_pull_request_rules_evaluator(
        self, ctxt: "context.Context"
    ) -> PullRequestRulesEvaluator:
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

        return await PullRequestRulesEvaluator.create(
            runtime_rules,
            ctxt.repository,
            [ctxt.pull_request],
            True,
        )


def CommandsRestrictionsSchema(
    command: type[actions_mod.Action],
) -> voluptuous.Schema:
    return {
        voluptuous.Required(
            "conditions", default=command.default_restrictions
        ): voluptuous.All(
            [
                voluptuous.Coerce(
                    lambda v: cond_config.RuleConditionSchema(
                        v, allow_command_attributes=True
                    )
                )
            ],
            voluptuous.Coerce(conditions_mod.PullRequestRuleConditions),
        )
    }


class CommandsRestrictions(typing.TypedDict):
    conditions: conditions_mod.PullRequestRuleConditions


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
                        [voluptuous.Coerce(cond_config.RuleConditionSchema)],
                        voluptuous.Coerce(conditions_mod.PullRequestRuleConditions),
                    ),
                    voluptuous.Required("actions"): actions_mod.get_action_schemas(),
                },
                voluptuous.Coerce(PullRequestRule.from_dict),
            ),
        ],
        voluptuous.Coerce(PullRequestRules),
    )


def FullifyPullRequestRules(
    v: "mergify_conf.MergifyConfig",
) -> "mergify_conf.MergifyConfig":
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
