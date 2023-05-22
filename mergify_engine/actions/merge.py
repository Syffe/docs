import dataclasses
import functools
import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine.actions import merge_base
from mergify_engine.actions import utils as action_utils
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules import conditions
from mergify_engine.rules import types
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.rules.config import queue_rules as qr_config


class MergeExecutorConfig(typing.TypedDict):
    method: merge_base.MergeMethodT
    commit_message_template: str | None
    merge_bot_account: github_types.GitHubLogin | None
    allow_merging_configuration_change: bool


@dataclasses.dataclass
class MergeExecutor(
    actions.ActionExecutor["MergeAction", "MergeExecutorConfig"],
    merge_base.MergeUtilsMixin,
):
    queue_rules: qr_config.QueueRules
    partition_rules: partr_config.PartitionRules

    @property
    def silenced_conclusion(self) -> tuple[check_api.Conclusion, ...]:
        return ()

    @classmethod
    async def create(
        cls,
        action: "MergeAction",
        ctxt: "context.Context",
        rule: "prr_config.EvaluatedPullRequestRule",
    ) -> "MergeExecutor":
        try:
            merge_bot_account = await action_utils.render_bot_account(
                ctxt,
                action.config["merge_bot_account"],
                bot_account_fallback=None,
                option_name="merge_bot_account",
            )
        except action_utils.RenderBotAccountFailure as e:
            raise actions.InvalidDynamicActionConfiguration(
                rule, action, e.title, e.reason
            )

        if action.config["method"] == "fast-forward":
            if action.config["commit_message_template"] is not None:
                raise actions.InvalidDynamicActionConfiguration(
                    rule,
                    action,
                    "Commit message can't be changed with fast-forward merge method",
                    "`commit_message_template` must not be set if `method: fast-forward` is set.",
                )

        return cls(
            ctxt,
            rule,
            MergeExecutorConfig(
                {
                    "method": action.config["method"],
                    "commit_message_template": action.config["commit_message_template"],
                    "merge_bot_account": merge_bot_account,
                    "allow_merging_configuration_change": action.config[
                        "allow_merging_configuration_change"
                    ],
                }
            ),
            action.queue_rules,
            action.partition_rules,
        )

    async def run(self) -> check_api.Result:
        report = await self.pre_merge_checks(
            self.ctxt,
            self.config["method"],
            self.config["merge_bot_account"],
        )
        if report is None:
            report = await self.common_merge(
                "merge",
                self.ctxt,
                self.config["method"],
                self.config["merge_bot_account"],
                self.config["commit_message_template"],
                functools.partial(
                    self.get_pending_merge_status,
                    rule=self.rule,
                ),
            )
            if report.conclusion == check_api.Conclusion.SUCCESS:
                convoy = await merge_train.Convoy.from_context(
                    self.ctxt, self.queue_rules, self.partition_rules
                )
                await convoy.remove_pull(
                    self.ctxt.pull["number"],
                    self.rule.get_signal_trigger(),
                    queue_utils.PrDequeued(
                        self.ctxt.pull["number"],
                        ". Pull request automatically merged by a `merge` action",
                    ),
                )

                await signals.send(
                    self.ctxt.repository,
                    self.ctxt.pull["number"],
                    "action.merge",
                    signals.EventMergeMetadata(
                        {"branch": self.ctxt.pull["base"]["ref"]}
                    ),
                    self.rule.get_signal_trigger(),
                )
        return report

    async def cancel(self) -> check_api.Result:
        return actions.CANCELLED_CHECK_REPORT

    async def get_pending_merge_status(
        self, ctxt: context.Context, rule: "prr_config.EvaluatedPullRequestRule"
    ) -> check_api.Result:
        return check_api.Result(
            check_api.Conclusion.PENDING, "The pull request will be merged soon", ""
        )


class MergeAction(actions.Action):
    flags = (
        actions.ActionFlag.DISALLOW_RERUN_ON_OTHER_RULES
        | actions.ActionFlag.SUCCESS_IS_FINAL_STATE
        # FIXME(sileht): MRGFY-562
        # enforce -merged/-closed in conditions requirements
        # | actions.ActionFlag.ALWAYS_RUN
    )

    validator = {
        voluptuous.Required("method", default="merge"): voluptuous.Any(
            *typing.get_args(merge_base.MergeMethodT)
        ),
        voluptuous.Required("merge_bot_account", default=None): types.Jinja2WithNone,
        voluptuous.Required(
            "commit_message_template", default=None
        ): types.Jinja2WithNone,
        voluptuous.Required("allow_merging_configuration_change", default=False): bool,
    }

    async def get_conditions_requirements(
        self, ctxt: context.Context
    ) -> list[conditions.RuleConditionNode]:
        conditions_requirements: list[conditions.RuleConditionNode] = []
        if self.config["method"] == "fast-forward":
            conditions_requirements.append(
                conditions.RuleCondition.from_tree(
                    {"=": ("#commits-behind", 0)},
                    description=":pushpin: fast-forward merge requirement",
                )
            )

        conditions_requirements.append(
            conditions.get_mergify_configuration_change_conditions(
                "merge", self.config["allow_merging_configuration_change"]
            )
        )
        conditions_requirements.append(
            conditions.RuleCondition.from_tree(
                {"=": ("draft", False)}, description=":pushpin: merge requirement"
            )
        )
        conditions_requirements.extend(
            await conditions.get_branch_protection_conditions(
                ctxt.repository, ctxt.pull["base"]["ref"], strict=True
            )
        )
        conditions_requirements.extend(await conditions.get_depends_on_conditions(ctxt))

        merge_after_condition = conditions.get_merge_after_condition(ctxt)
        if merge_after_condition is not None:
            conditions_requirements.append(merge_after_condition)

        return conditions_requirements

    executor_class = MergeExecutor

    # NOTE(sileht): set by validate_config()
    queue_rules: qr_config.QueueRules = dataclasses.field(init=False, repr=False)
    partition_rules: partr_config.PartitionRules = dataclasses.field(
        init=False, repr=False
    )

    def validate_config(self, mergify_config: "mergify_conf.MergifyConfig") -> None:
        self.queue_rules = mergify_config["queue_rules"]
        self.partition_rules = mergify_config["partition_rules"]
