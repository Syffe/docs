from __future__ import annotations

from collections import abc
import dataclasses
import typing

import daiquiri
from ddtrace import tracer
import voluptuous

from mergify_engine import actions as actions_mod
from mergify_engine import github_types
from mergify_engine import yaml
from mergify_engine.rules import conditions as conditions_mod
from mergify_engine.rules import live_resolvers
from mergify_engine.rules import types
from mergify_engine.rules.config import defaults as defaults_config


if typing.TYPE_CHECKING:
    from mergify_engine import context

LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class GenericRulesEvaluator(typing.Generic[types.T_Rule, types.T_EvaluatedRule]):
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
    rules: list[types.T_Rule]

    # The rules matching the pull request.
    matching_rules: list[types.T_EvaluatedRule] = dataclasses.field(
        init=False, default_factory=list
    )

    # The rules that can't be computed due to runtime error (eg: team resolution failure)
    faulty_rules: list[types.T_EvaluatedRule] = dataclasses.field(
        init=False, default_factory=list
    )

    # The rules not matching the pull request.
    ignored_rules: list[types.T_EvaluatedRule] = dataclasses.field(
        init=False, default_factory=list
    )

    # The rules not matching the base changeable attributes
    not_applicable_base_changeable_attributes_rules: list[
        types.T_EvaluatedRule
    ] = dataclasses.field(init=False, default_factory=list)

    @classmethod
    async def create(
        cls,
        rules: list[types.T_Rule],
        repository: context.Repository,
        pulls: list[context.BasePullRequest],
        rule_hidden_from_merge_queue: bool,
    ) -> GenericRulesEvaluator[types.T_Rule, types.T_EvaluatedRule]:
        # Circular import
        from mergify_engine.rules.config import queue_rules as qr_config

        self = cls(rules)

        for rule in self.rules:
            apply_configure_filter(repository, rule.conditions)
            evaluated_rule = typing.cast(types.T_EvaluatedRule, await rule.evaluate(pulls))  # type: ignore[redundant-cast]
            if isinstance(rule, qr_config.QueueRule):
                apply_configure_filter(repository, rule.routing_conditions)
            del rule

            # NOTE(sileht):
            # In the summary, we display rules in five groups:
            # * rules where all attributes match -> matching_rules
            # * rules where only BASE_ATTRIBUTES match (filter out rule written for other branches) -> matching_rules
            # * rules that won't work due to a configuration issue detected at runtime (eg team doesn't exists) -> faulty_rules
            # * rules where only BASE_ATTRIBUTES don't match (mainly to hide rule written for other branches) -> ignored_rules
            # * rules where only BASE_CHANGEABLE ATTRIBUTES don't match (they rarely change but are handled)-> not_applicable_base_changeable_attributes_rules
            categorized = False
            evaluated_rule_conditions = evaluated_rule.conditions

            # NOTE(Syffe): routing_conditions status can change even after the PR is queued.
            # Thus we need to evaluate conditions and routing_conditions together when they are available
            # since not matching routing_conditions are a motive for PR invalidity
            if isinstance(evaluated_rule, qr_config.QueueRule):
                evaluated_rule_conditions = conditions_mod.QueueRuleMergeConditions(
                    evaluated_rule.conditions.condition.copy().conditions
                    + evaluated_rule.routing_conditions.condition.copy().conditions
                )

            if rule_hidden_from_merge_queue and not evaluated_rule_conditions.match:
                # NOTE(sileht): Replace non-base attribute and non-base changeables attributes
                # by true, if it still matches it's a potential rule otherwise hide it.
                base_changeable_conditions = evaluated_rule_conditions.copy()
                for condition in base_changeable_conditions.walk():
                    attr = condition.get_attribute_name()
                    if attr not in self.BASE_CHANGEABLE_ATTRIBUTES:
                        condition.make_always_true()

                base_conditions = evaluated_rule_conditions.copy()
                for condition in base_conditions.walk():
                    attr = condition.get_attribute_name()
                    if attr not in self.BASE_ATTRIBUTES:
                        condition.make_always_true()

                await base_changeable_conditions(pulls)
                await base_conditions(pulls)

                if not base_changeable_conditions.match:
                    self.not_applicable_base_changeable_attributes_rules.append(
                        evaluated_rule
                    )

                if not base_conditions.match:
                    self.ignored_rules.append(evaluated_rule)
                    categorized = True

                if not categorized and evaluated_rule_conditions.is_faulty():
                    self.faulty_rules.append(evaluated_rule)
                    categorized = True

            if not categorized:
                self.matching_rules.append(evaluated_rule)

        return self


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


def UserConfigurationSchema(
    config: dict[str, typing.Any], partial_validation: bool = False
) -> voluptuous.Schema:
    # Circular import
    from mergify_engine.rules.config import partition_rules as partr_config
    from mergify_engine.rules.config import pull_request_rules as prr_config
    from mergify_engine.rules.config import queue_rules as qr_config

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
        ): prr_config.get_pull_request_rules_schema(),
        voluptuous.Required(
            "queue_rules",
            default=[
                {
                    "name": "default",
                    "priority_rules": [],
                    "merge_conditions": [],
                    "routing_conditions": [],
                }
            ],
        ): qr_config.QueueRulesSchema,
        voluptuous.Required(
            "partition_rules", default=[]
        ): partr_config.PartitionRulesSchema,
        voluptuous.Required("commands_restrictions", default={}): {
            voluptuous.Required(
                name, default={}
            ): prr_config.CommandsRestrictionsSchema(command)
            for name, command in actions_mod.get_commands().items()
        },
        voluptuous.Required(
            "defaults", default={}
        ): defaults_config.get_defaults_schema(),
        voluptuous.Remove("shared"): voluptuous.Any(dict, list, str, int, float, bool),
    }

    if not partial_validation:
        schema = voluptuous.And(
            schema, voluptuous.Coerce(prr_config.FullifyPullRequestRules)
        )

    return voluptuous.Schema(schema)(config)


YamlSchema: abc.Callable[[str], typing.Any] = voluptuous.Schema(voluptuous.Coerce(YAML))


def apply_configure_filter(
    repository: context.Repository,
    conditions: (
        conditions_mod.PullRequestRuleConditions
        | conditions_mod.QueueRuleMergeConditions
        | conditions_mod.PriorityRuleConditions
        | conditions_mod.PartitionRuleConditions
    ),
) -> None:
    for condition in conditions.walk():
        live_resolvers.configure_filter(repository, condition.filters.boolean)
        live_resolvers.configure_filter(repository, condition.filters.next_evaluation)
        if condition.filters.related_checks is not None:
            live_resolvers.configure_filter(
                repository, condition.filters.related_checks
            )
