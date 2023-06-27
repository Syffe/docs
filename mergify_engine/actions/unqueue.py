import dataclasses
import typing

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import dashboard
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.rules.config import queue_rules as qr_config


class UnqueueExecutorConfig(typing.TypedDict):
    pass


@dataclasses.dataclass
class UnqueueExecutor(
    actions.ActionExecutor["UnqueueCommand", "UnqueueExecutorConfig"]
):
    queue_rules: qr_config.QueueRules
    partition_rules: partr_config.PartitionRules

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
            action.partition_rules,
        )

    async def run(self) -> check_api.Result:
        convoy = await merge_train.Convoy.from_context(
            self.ctxt, self.queue_rules, self.partition_rules
        )
        queue_name = await convoy.get_queue_name_from_pull_request_number(
            self.ctxt.pull["number"]
        )
        if queue_name is None:
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
                title=f"The pull request has been removed from the queue `{queue_name}` by an `unqueue` command",
                summary="",
            ),
            details_url=await dashboard.get_queues_url_from_context(self.ctxt, convoy),
        )
        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            "action.unqueue",
            signals.EventNoMetadata(),
            self.rule.get_signal_trigger(),
        )

        await convoy.remove_pull(
            self.ctxt.pull["number"],
            self.rule.get_signal_trigger(),
            queue_utils.PrDequeued(
                self.ctxt.pull["number"], " by an `unqueue` command."
            ),
        )
        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            title=f"The pull request has been removed from the queue `{queue_name}`",
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

    required_feature_for_command = subscription.Features.MERGE_QUEUE

    queue_rules: qr_config.QueueRules = dataclasses.field(init=False, repr=False)
    partition_rules: partr_config.PartitionRules = dataclasses.field(
        init=False, repr=False
    )

    def validate_config(self, mergify_config: "mergify_conf.MergifyConfig") -> None:
        self.queue_rules = mergify_config["queue_rules"]
        self.partition_rules = mergify_config["partition_rules"]
