import typing

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import rules
from mergify_engine import signals
from mergify_engine.queue import merge_train


class UnqueueExecutorConfig(typing.TypedDict):
    pass


class UnqueueExecutor(
    actions.ActionExecutor["UnqueueCommand", "UnqueueExecutorConfig"]
):
    @classmethod
    async def create(
        cls,
        action: "UnqueueCommand",
        ctxt: "context.Context",
        rule: "rules.EvaluatedRule",
    ) -> "UnqueueExecutor":
        return cls(
            ctxt,
            rule,
            UnqueueExecutorConfig(),
        )

    async def run(self) -> check_api.Result:
        train = await merge_train.Train.from_context(self.ctxt)
        _, embarked_pull = train.find_embarked_pull(self.ctxt.pull["number"])
        if embarked_pull is None:
            return check_api.Result(
                check_api.Conclusion.NEUTRAL,
                title="The pull request is not queued",
                summary="",
            )

        # manually set a status, to not automatically re-embark it
        await check_api.set_check_run(
            self.ctxt,
            constants.MERGE_QUEUE_SUMMARY_NAME,
            check_api.Result(
                check_api.Conclusion.CANCELLED,
                title="The pull request has been removed from the queue by an `unqueue` command",
                summary="",
            ),
        )
        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            "action.unqueue",
            signals.EventNoMetadata(),
            self.rule.get_signal_trigger(),
        )
        await train.remove_pull(
            self.ctxt,
            self.rule.get_signal_trigger(),
            "The unqueue command has been ran",
        )
        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            title="The pull request has been removed from the queue",
            summary="",
        )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT


class UnqueueCommand(actions.Action):
    flags = actions.ActionFlag.ALLOW_ON_CONFIGURATION_CHANGED

    validator: typing.ClassVar[dict[typing.Any, typing.Any]] = {}
    executor_class = UnqueueExecutor

    default_restrictions: typing.ClassVar[list[typing.Any]] = [
        "sender-permission>=write"
    ]
