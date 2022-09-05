import typing

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import refresher
from mergify_engine import rules
from mergify_engine import signals


class RefreshCommand(actions.BackwardCompatAction):
    flags = actions.ActionFlag.ALLOW_ON_CONFIGURATION_CHANGED

    validator: typing.ClassVar[typing.Dict[typing.Any, typing.Any]] = {}

    async def run(
        self, ctxt: context.Context, rule: rules.EvaluatedRule
    ) -> check_api.Result:
        await refresher.send_pull_refresh(
            ctxt.redis.stream,
            ctxt.pull["base"]["repo"],
            pull_request_number=ctxt.pull["number"],
            action="user",
            source="action/command",
        )
        await signals.send(
            ctxt.repository,
            ctxt.pull["number"],
            "action.refresh",
            signals.EventNoMetadata(),
            rule.get_signal_trigger(),
        )
        return check_api.Result(
            check_api.Conclusion.SUCCESS, title="Pull request refreshed", summary=""
        )

    async def cancel(
        self, ctxt: context.Context, rule: "rules.EvaluatedRule"
    ) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT
