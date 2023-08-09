import typing

import voluptuous

from mergify_engine.rules import conditions as conditions_mod


def RuleConditionSchema(
    v: typing.Any, depth: int = 0, allow_command_attributes: bool = False
) -> typing.Any:
    if depth > 8:
        raise voluptuous.Invalid("Maximum number of nested conditions reached")

    return voluptuous.Schema(
        voluptuous.Any(
            voluptuous.All(
                str,
                voluptuous.Coerce(
                    lambda v: conditions_mod.RuleCondition.from_string(
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
                        voluptuous.Length(min=1),
                    ),
                    "or": voluptuous.All(
                        [
                            lambda v: RuleConditionSchema(
                                v,
                                depth + 1,
                                allow_command_attributes=allow_command_attributes,
                            )
                        ],
                        voluptuous.Length(min=1),
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
