import typing

from mergify_engine import actions
from mergify_engine import context
from mergify_engine import duplicate_pull
from mergify_engine.actions import copy
from mergify_engine.rules import conditions


class BackportExecutor(copy.CopyExecutor):
    KIND: duplicate_pull.KindT = "backport"
    HOOK_EVENT_NAME: typing.Literal[
        "action.backport", "action.copy"
    ] = "action.backport"
    BRANCH_PREFIX: str = "bp"
    SUCCESS_MESSAGE: str = "Backports have been created"
    FAILURE_MESSAGE: str = "No backport have been created"


class BackportAction(copy.CopyAction):
    flags = (
        actions.ActionFlag.ALLOW_ON_CONFIGURATION_CHANGED
        | actions.ActionFlag.ALLOW_AS_PENDING_COMMAND
    )
    executor_class = BackportExecutor

    KIND: duplicate_pull.KindT = "backport"

    async def get_conditions_requirements(
        self, ctxt: context.Context
    ) -> list[conditions.RuleConditionNode]:
        return [
            conditions.RuleCondition(
                "merged", description=":pushpin: backport requirement"
            )
        ]
