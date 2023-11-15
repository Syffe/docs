from __future__ import annotations

import typing

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import refresher
from mergify_engine import signals


if typing.TYPE_CHECKING:
    from mergify_engine import context
    from mergify_engine.rules.config import pull_request_rules as prr_config


class RefreshExecutorConfig(typing.TypedDict):
    pass


class RefreshExecutor(actions.ActionExecutor["RefreshCommand", RefreshExecutorConfig]):
    @classmethod
    async def create(
        cls,
        action: RefreshCommand,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> RefreshExecutor:
        return cls(ctxt, rule, RefreshExecutorConfig())

    async def run(self) -> check_api.Result:
        await refresher.send_pull_refresh(
            self.ctxt.redis.stream,
            self.ctxt.pull["base"]["repo"],
            pull_request_number=self.ctxt.pull["number"],
            action="user",
            source="action/command",
        )
        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            self.ctxt.pull["base"]["ref"],
            "action.refresh",
            signals.EventNoMetadata(),
            self.rule.get_signal_trigger(),
        )
        return check_api.Result(
            check_api.Conclusion.SUCCESS, title="Pull request refreshed", summary=""
        )

    async def cancel(self) -> check_api.Result:
        return actions.CANCELLED_CHECK_REPORT


class RefreshCommand(actions.Action):
    flags = actions.ActionFlag.NONE

    validator: typing.ClassVar[dict[typing.Any, typing.Any]] = {}
    executor_class = RefreshExecutor

    default_restrictions: typing.ClassVar[list[typing.Any]] = [
        {"or": ["sender-permission>=write", "sender={{author}}"]}
    ]
