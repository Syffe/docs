import typing

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import duplicate_pull
from mergify_engine.actions import copy
from mergify_engine.rules import conditions


class BackportAction(copy.CopyAction):
    flags = (
        actions.ActionFlag.ALLOW_ON_CONFIGURATION_CHANGED
        | actions.ActionFlag.ALLOW_AS_PENDING_COMMAND
    )

    KIND: duplicate_pull.KindT = "backport"
    HOOK_EVENT_NAME: typing.Literal[
        "action.backport", "action.copy"
    ] = "action.backport"
    BRANCH_PREFIX: str = "bp"
    SUCCESS_MESSAGE: str = "Backports have been created"
    FAILURE_MESSAGE: str = "No backport have been created"

    default_restrictions: typing.ClassVar[list[typing.Any]] = [
        "sender-permission>=write"
    ]

    @property
    def silenced_conclusion(self) -> tuple[check_api.Conclusion, ...]:
        return ()

    @staticmethod
    def command_to_config(string: str) -> dict[str, typing.Any]:
        if string:
            return {"branches": string.split(" ")}
        else:
            return {}

    async def get_conditions_requirements(
        self, ctxt: context.Context
    ) -> list[conditions.RuleConditionNode]:
        return [
            conditions.RuleCondition(
                "merged", description=":pushpin: backport requirement"
            )
        ]
