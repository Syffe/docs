from __future__ import annotations

import typing

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import duplicate_pull
from mergify_engine.actions import copy
from mergify_engine.rules import conditions


if typing.TYPE_CHECKING:
    from mergify_engine import context


class BackportExecutor(copy.CopyExecutor):
    KIND: duplicate_pull.KindT = "backport"
    HOOK_EVENT_NAME: typing.Literal[
        "action.backport", "action.copy"
    ] = "action.backport"
    BRANCH_PREFIX: str = "bp"
    SUCCESS_MESSAGE: str = "Backports have been created"
    FAILURE_MESSAGE: str = "No backport have been created"

    @property
    def silenced_conclusion(self) -> tuple[check_api.Conclusion, ...]:
        return ()


class BackportAction(copy.CopyAction):
    flags = actions.ActionFlag.ALLOW_AS_PENDING_COMMAND
    executor_class = BackportExecutor

    KIND: duplicate_pull.KindT = "backport"
    KIND_PLURAL = "backports"

    @staticmethod
    def command_to_config(string: str) -> dict[str, typing.Any]:
        if string:
            return {"branches": string.split(" ")}
        return {}

    async def get_conditions_requirements(
        self, ctxt: context.Context
    ) -> list[conditions.RuleConditionNode]:
        return [
            conditions.RuleCondition.from_tree(
                {"=": ("merged", True)}, description=":pushpin: backport requirement"
            )
        ]
