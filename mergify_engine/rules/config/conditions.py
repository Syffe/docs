import typing

import voluptuous

from mergify_engine.rules import conditions as conditions_mod
from mergify_engine.rules import types


MAX_CONDITIONS_PER_GROUP = 500


def DeferSchemaLoading(
    schema: typing.Any,
    *args: typing.Any,
    **kwargs: typing.Any,
) -> typing.Any:
    # NOTE(sileht): Loading the schema recursivly is slow as hell in voluptuous
    # This is an helper to break the recursivity and create many smaller schema
    # This is not a perfect solution, as reporting error will be less precise
    # but it's good enough for our use case

    # ruff complains but this is the goal of this helper create a lambda to not load the schema until it's evaluated
    return lambda v: voluptuous.Schema(schema(*args, **kwargs))(v)  # noqa: PLW0108


def NestedRuleConditionReached(_v: typing.Any) -> typing.Any:
    raise voluptuous.Invalid("Maximum number of nested conditions reached")


def ListOfRuleCondition(
    depth: int = 0,
    allow_command_attributes: bool = False,
    _min: int = 0,
) -> typing.Any:
    return types.ListOf(
        DeferSchemaLoading(RuleCondition, depth, allow_command_attributes),
        _min=_min,
        _max=MAX_CONDITIONS_PER_GROUP,
    )


def RuleCondition(
    depth: int = 0,
    allow_command_attributes: bool = False,
) -> typing.Any:
    if depth > 8:
        return NestedRuleConditionReached

    return voluptuous.Any(
        voluptuous.All(
            str,
            voluptuous.Coerce(
                lambda v: conditions_mod.RuleCondition.from_string(
                    v,
                    allow_command_attributes=allow_command_attributes,
                ),
            ),
        ),
        voluptuous.All(
            {
                "and": ListOfRuleCondition(depth + 1, allow_command_attributes, 1),
                "or": ListOfRuleCondition(depth + 1, allow_command_attributes, 1),
            },
            voluptuous.Length(min=1, max=1),
            voluptuous.Coerce(conditions_mod.RuleConditionCombination),
        ),
        voluptuous.All(
            {
                "not": DeferSchemaLoading(
                    RuleCondition,
                    depth + 1,
                    allow_command_attributes,
                ),
            },
            voluptuous.Length(min=1, max=1),
            voluptuous.Coerce(conditions_mod.RuleConditionNegation),
        ),
    )
