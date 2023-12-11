from __future__ import annotations

import typing

import daiquiri
import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import dashboard
from mergify_engine import refresher
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine.engine import commands_runner


if typing.TYPE_CHECKING:
    from mergify_engine import context
    from mergify_engine.rules.config import pull_request_rules as prr_config
    from mergify_engine.rules.config import queue_rules as qr_config

LOG = daiquiri.getLogger(__name__)


class RequeueExecutorConfig(typing.TypedDict):
    queue_name: qr_config.QueueName | None


class RequeueExecutor(
    actions.ActionExecutor["RequeueCommand", "RequeueExecutorConfig"],
):
    @classmethod
    async def create(
        cls,
        action: RequeueCommand,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> RequeueExecutor:
        return cls(
            ctxt,
            rule,
            RequeueExecutorConfig({"queue_name": action.config["queue_name"]}),
        )

    async def run(self) -> check_api.Result | commands_runner.FollowUpCommand:
        previous_queue_check = await self.ctxt.get_merge_queue_check_run()
        if not previous_queue_check:
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                title="This pull request head commit has not been previously disembarked from queue.",
                summary="",
            )

        if check_api.Conclusion(previous_queue_check["conclusion"]) in [
            check_api.Conclusion.SUCCESS,
            check_api.Conclusion.NEUTRAL,
            check_api.Conclusion.PENDING,
        ]:
            return check_api.Result(
                check_api.Conclusion.NEUTRAL,
                title="This pull request is already queued",
                summary="",
            )

        has_queue_action_in_prr = any(
            "queue" in prr.actions
            for prr in self.ctxt.repository.mergify_config["pull_request_rules"]
        )

        # If the config has a `queue` action in the pull_request_rules
        # then we just mark the pull request as re-embarkable and the
        # rules will take care of automatically re-adding it in the queue.
        if has_queue_action_in_prr:
            self.ctxt.log.debug(
                "requeue command marks the pull request as re-embarkable",
                check=previous_queue_check,
            )
            return await self._mark_pull_request_as_reembarkable()

        # Otherwise we need to manually execute the queue action again
        await check_api.set_check_run(
            self.ctxt,
            await self.ctxt.get_merge_queue_check_run_name(),
            check_api.Result(
                check_api.Conclusion.NEUTRAL,
                "This pull request will be re-embarked automatically",
                "",
            ),
            details_url=await dashboard.get_queues_url_from_context(self.ctxt),
        )
        queue_command = "queue"
        if self.config["queue_name"] is not None:
            queue_command += f" {self.config['queue_name']}"

        return commands_runner.FollowUpCommand(
            queue_command,
            check_api.Result(
                check_api.Conclusion.SUCCESS,
                title="This pull request will be re-embarked automatically",
                summary=f"The followup `{queue_command}` command will be automatically executed to re-embark the pull request",
            ),
        )

    async def _mark_pull_request_as_reembarkable(self) -> check_api.Result:
        await check_api.set_check_run(
            self.ctxt,
            await self.ctxt.get_merge_queue_check_run_name(),
            check_api.Result(
                check_api.Conclusion.NEUTRAL,
                "This pull request can be re-embarked automatically",
                "",
            ),
            details_url=await dashboard.get_queues_url_from_context(self.ctxt),
        )

        # NOTE(sileht): refresh it to, maybe, retrigger the queue action.
        await refresher.send_pull_refresh(
            self.ctxt.redis.stream,
            self.ctxt.pull["base"]["repo"],
            pull_request_number=self.ctxt.pull["number"],
            action="user",
            source="action/command/requeue",
        )

        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            self.ctxt.pull["base"]["ref"],
            "action.requeue",
            signals.EventNoMetadata(),
            self.rule.get_signal_trigger(),
        )

        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            title="The queue state of this pull request has been cleaned. It can be re-embarked automatically",
            summary="",
        )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT


class RequeueCommand(actions.Action):
    flags = actions.ActionFlag.NONE

    validator: typing.ClassVar[dict[typing.Any, typing.Any]] = {
        voluptuous.Required("queue_name", default=None): voluptuous.Any(str, None),
    }

    executor_class = RequeueExecutor

    required_feature_for_command = subscription.Features.MERGE_QUEUE

    @staticmethod
    def command_to_config(command_arguments: str) -> dict[str, typing.Any]:
        name: str | None = None
        config = {"queue_name": name}
        args = [
            arg_stripped
            for arg in command_arguments.split(" ")
            if (arg_stripped := arg.strip())
        ]
        if args and args[0]:
            config["queue_name"] = args[0]
        return config
