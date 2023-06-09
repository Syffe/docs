import typing

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import dashboard
from mergify_engine import refresher
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine.rules.config import pull_request_rules as prr_config


class RequeueExecutorConfig(typing.TypedDict):
    pass


class RequeueExecutor(
    actions.ActionExecutor["RequeueCommand", "RequeueExecutorConfig"]
):
    @classmethod
    async def create(
        cls,
        action: "RequeueCommand",
        ctxt: "context.Context",
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> "RequeueExecutor":
        return cls(ctxt, rule, RequeueExecutorConfig())

    async def run(self) -> check_api.Result:
        check = await self.ctxt.get_engine_check_run(constants.MERGE_QUEUE_SUMMARY_NAME)
        if not check:
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                title="This pull request head commit has not been previously disembarked from queue.",
                summary="",
            )

        if check_api.Conclusion(check["conclusion"]) in [
            check_api.Conclusion.SUCCESS,
            check_api.Conclusion.NEUTRAL,
            check_api.Conclusion.PENDING,
        ]:
            return check_api.Result(
                check_api.Conclusion.NEUTRAL,
                title="This pull request is already queued",
                summary="",
            )

        self.ctxt.log.info(
            "requeue command marks the pull request as re-embarkable", check=check
        )

        await check_api.set_check_run(
            self.ctxt,
            constants.MERGE_QUEUE_SUMMARY_NAME,
            check_api.Result(
                check_api.Conclusion.NEUTRAL,
                "This pull request can be re-embarked automatically",
                "",
            ),
            details_url=await dashboard.get_queues_url_from_context(self.ctxt),
        )

        # NOTE(sileht): refresh it to maybe, retrigger the queue action.
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

    validator: typing.ClassVar[dict[typing.Any, typing.Any]] = {}

    executor_class = RequeueExecutor

    default_restrictions: typing.ClassVar[list[typing.Any]] = [
        "sender-permission>=write"
    ]

    required_feature_for_command = subscription.Features.MERGE_QUEUE
