import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import signals
from mergify_engine.clients import http
from mergify_engine.rules import types
from mergify_engine.rules.config import pull_request_rules as prr_config


MSG = "This pull request has been automatically closed by Mergify."


class CloseExecutorConfig(typing.TypedDict):
    message: str


class CloseExecutor(actions.ActionExecutor["CloseAction", CloseExecutorConfig]):
    @classmethod
    async def create(
        cls,
        action: "CloseAction",
        ctxt: "context.Context",
        rule: "prr_config.EvaluatedPullRequestRule",
    ) -> "CloseExecutor":
        try:
            message = await ctxt.pull_request.render_template(action.config["message"])
        except context.RenderTemplateFailure as rmf:
            raise actions.InvalidDynamicActionConfiguration(
                rule, action, "Invalid close message", str(rmf)
            )
        return cls(ctxt, rule, CloseExecutorConfig({"message": message}))

    async def run(self) -> check_api.Result:
        if self.ctxt.closed:
            return check_api.Result(
                check_api.Conclusion.SUCCESS, "Pull request is already closed", ""
            )

        try:
            await self.ctxt.client.patch(
                f"{self.ctxt.base_url}/pulls/{self.ctxt.pull['number']}",
                json={"state": "close"},
            )
        except http.HTTPClientSideError as e:  # pragma: no cover
            return check_api.Result(
                check_api.Conclusion.FAILURE, "Pull request can't be closed", e.message
            )

        try:
            await self.ctxt.client.post(
                f"{self.ctxt.base_url}/issues/{self.ctxt.pull['number']}/comments",
                json={"body": self.config["message"]},
            )
        except http.HTTPClientSideError as e:  # pragma: no cover
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                "The close message can't be created",
                e.message,
            )

        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            "action.close",
            signals.EventCloseMetadata(message=self.config["message"]),
            self.rule.get_signal_trigger(),
        )
        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            "The pull request has been closed",
            self.config["message"],
        )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT

    @property
    def silenced_conclusion(self) -> tuple[check_api.Conclusion, ...]:
        return ()


class CloseAction(actions.Action):
    flags = actions.ActionFlag.DISALLOW_RERUN_ON_OTHER_RULES
    validator: typing.ClassVar[actions.ValidatorT] = {
        voluptuous.Required("message", default=MSG): types.Jinja2
    }
    executor_class = CloseExecutor
