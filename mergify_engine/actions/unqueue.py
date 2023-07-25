import dataclasses
import typing

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import dashboard
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine.engine import commands_runner
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
            # Try to find any queue command in waiting state (waiting for queue conditions to be valid).
            # In this state, the PR cannot be in any train yet, so we need to edit the queue comment,
            # if there is a queue comment response already otherwise we just post a new comment,
            # to cancel this waiting state.
            # This will prevent the PR from being queued if the queue conditions matches later on.
            pendings = await commands_runner.get_pending_commands_to_run_from_comments(
                self.ctxt
            )
            has_pending_queue = False
            for command, state in pendings.items():
                # Use `startswith` to include commands with arguments
                if not command.startswith("queue"):
                    continue

                has_pending_queue = True

                message = commands_runner.prepare_message(
                    command=command,
                    result=check_api.Result(
                        conclusion=check_api.Conclusion.CANCELLED,
                        title="This `queue` command has been cancelled by an `unqueue` command",
                        summary="",
                    ),
                    action_is_running=True,
                )

                if state.github_comment_result is None:
                    # Means this is the first time running the command and we haven't responded yet.
                    await self.ctxt.post_comment(message)
                else:
                    await self.ctxt.edit_comment(
                        state.github_comment_result["id"], message
                    )

            if not has_pending_queue:
                return check_api.Result(
                    check_api.Conclusion.NEUTRAL,
                    title="The pull request is not queued",
                    summary="",
                )

            return check_api.Result(
                check_api.Conclusion.SUCCESS,
                title="The pull request is not waiting to be queued anymore.",
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
