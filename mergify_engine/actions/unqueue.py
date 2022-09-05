import typing

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import rules
from mergify_engine import signals
from mergify_engine.queue import merge_train


class UnqueueCommand(actions.BackwardCompatAction):
    flags = actions.ActionFlag.ALLOW_ON_CONFIGURATION_CHANGED

    validator: typing.ClassVar[typing.Dict[typing.Any, typing.Any]] = {}

    async def run(
        self, ctxt: context.Context, rule: rules.EvaluatedRule
    ) -> check_api.Result:
        train = await merge_train.Train.from_context(ctxt)
        _, embarked_pull = train.find_embarked_pull(ctxt.pull["number"])
        if embarked_pull is None:
            return check_api.Result(
                check_api.Conclusion.NEUTRAL,
                title="The pull request is not queued",
                summary="",
            )

        # manually set a status, to not automatically re-embark it
        await check_api.set_check_run(
            ctxt,
            constants.MERGE_QUEUE_SUMMARY_NAME,
            check_api.Result(
                check_api.Conclusion.CANCELLED,
                title="The pull request has been removed from the queue by an `unqueue` command",
                summary="",
            ),
        )
        await signals.send(
            ctxt.repository,
            ctxt.pull["number"],
            "action.unqueue",
            signals.EventNoMetadata(),
            rule.get_signal_trigger(),
        )
        await train.remove_pull(
            ctxt, rule.get_signal_trigger(), "The unqueue command has been ran"
        )
        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            title="The pull request has been removed from the queue",
            summary="",
        )

    async def cancel(
        self, ctxt: context.Context, rule: "rules.EvaluatedRule"
    ) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT
