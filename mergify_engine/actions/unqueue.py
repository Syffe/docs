import dataclasses
import typing

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import signals
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.rules.config import queue_rules as qr_config


class UnqueueExecutorConfig(typing.TypedDict):
    pass


@dataclasses.dataclass
class UnqueueExecutor(
    actions.ActionExecutor["UnqueueCommand", "UnqueueExecutorConfig"]
):
    queue_rules: qr_config.QueueRules

    @classmethod
    async def create(
        cls,
        action: "UnqueueCommand",
        ctxt: "context.Context",
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> "UnqueueExecutor":
        return cls(
            ctxt,
            rule,
            UnqueueExecutorConfig(),
            action.queue_rules,
        )

    async def run(self) -> check_api.Result:
        train = await merge_train.Train.from_context(self.ctxt, self.queue_rules)
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
            queue_utils.PrDequeued(
                self.ctxt.pull["number"], " by an `unqueue` command."
            ),
        )
        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            title="The pull request has been removed from the queue",
            summary="",
        )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT


class UnqueueCommand(actions.Action):
    flags = actions.ActionFlag.NONE

    validator: typing.ClassVar[dict[typing.Any, typing.Any]] = {}
    executor_class = UnqueueExecutor

    default_restrictions: typing.ClassVar[list[typing.Any]] = [
        "sender-permission>=write"
    ]

    queue_rules: qr_config.QueueRules = dataclasses.field(init=False, repr=False)

    def validate_config(self, mergify_config: "mergify_conf.MergifyConfig") -> None:
        self.queue_rules = mergify_config["queue_rules"]
