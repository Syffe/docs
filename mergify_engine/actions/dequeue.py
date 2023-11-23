from __future__ import annotations

import dataclasses
import typing

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import dashboard
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine.engine import commands_runner
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils


if typing.TYPE_CHECKING:
    from mergify_engine import context
    from mergify_engine.rules.config import pull_request_rules as prr_config


class DequeueExecutorConfig(typing.TypedDict):
    pass


@dataclasses.dataclass
class DequeueExecutor(
    actions.ActionExecutor["DequeueCommand", "DequeueExecutorConfig"],
):
    @classmethod
    async def create(
        cls,
        action: DequeueCommand,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> DequeueExecutor:
        return cls(
            ctxt,
            rule,
            DequeueExecutorConfig(),
        )

    async def run(self) -> check_api.Result:
        convoy = await merge_train.Convoy.from_context(self.ctxt)
        queue_name = await convoy.get_queue_name_from_pull_request_number(
            self.ctxt.pull["number"],
        )
        if queue_name is None:
            # Try to find any queue command in waiting state (waiting for queue conditions to be valid).
            # In this state, the PR cannot be in any train yet, so we need to edit the queue comment,
            # if there is a queue comment response already otherwise we just post a new comment,
            # to cancel this waiting state.
            # This will prevent the PR from being queued if the queue conditions matches later on.
            pendings = await commands_runner.get_pending_commands_to_run_from_comments(
                self.ctxt,
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
                        title="This `queue` command has been cancelled by a `dequeue` command",
                        summary="",
                    ),
                    action_is_running=True,
                )

                if state.github_comment_result is None:
                    # Means this is the first time running the command and we haven't responded yet.
                    await self.ctxt.post_comment(message)
                else:
                    await self.ctxt.edit_comment(
                        state.github_comment_result["id"],
                        message,
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
            await self.ctxt.get_merge_queue_check_run_name(),
            check_api.Result(
                check_api.Conclusion.CANCELLED,
                title=f"The pull request has been removed from the queue `{queue_name}` by a `dequeue` command",
                summary="",
            ),
            details_url=await dashboard.get_queues_url_from_context(self.ctxt, convoy),
        )
        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            self.ctxt.pull["base"]["ref"],
            "action.unqueue",
            signals.EventNoMetadata(),
            self.rule.get_signal_trigger(),
        )

        await convoy.remove_pull(
            self.ctxt.pull["number"],
            self.rule.get_signal_trigger(),
            queue_utils.PrDequeued(
                self.ctxt.pull["number"],
                " by a `dequeue` command.",
            ),
        )
        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            title=f"The pull request has been removed from the queue `{queue_name}`",
            summary="",
        )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT


class DequeueCommand(actions.Action):
    flags = actions.ActionFlag.NONE

    validator: typing.ClassVar[dict[typing.Any, typing.Any]] = {}
    executor_class = DequeueExecutor

    default_restrictions: typing.ClassVar[list[typing.Any]] = [
        "sender-permission>=write",
    ]

    required_feature_for_command = subscription.Features.MERGE_QUEUE
