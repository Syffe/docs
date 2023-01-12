from collections import abc
import re
import typing
from urllib import parse

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import duplicate_pull
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine import signals
from mergify_engine.actions import utils as action_utils
from mergify_engine.clients import http
from mergify_engine.dashboard import subscription
from mergify_engine.dashboard import user_tokens
from mergify_engine.rules import types


def Regex(value: str) -> re.Pattern[str]:
    try:
        return re.compile(value)
    except re.error as e:
        raise voluptuous.Invalid(str(e))


DUPLICATE_BODY_EXTRA_VARIABLES: dict[str, str | bool] = {
    "destination_branch": "branch-name-example",
    "cherry_pick_error": "cherry-pick error message example",
}

DUPLICATE_TITLE_EXTRA_VARIABLES: dict[str, str | bool] = {
    "destination_branch": "branch-name-example",
}


def DuplicateBodyJinja2(v: typing.Any) -> str | None:
    return types.Jinja2(v, DUPLICATE_BODY_EXTRA_VARIABLES)


def DuplicateTitleJinja2(v: typing.Any) -> str | None:
    return types.Jinja2(v, DUPLICATE_TITLE_EXTRA_VARIABLES)


class CopyExecutorConfig(typing.TypedDict):
    bot_account: user_tokens.UserTokensUser | None
    branches: list[github_types.GitHubRefType]
    ignore_conflicts: bool
    assignees: list[str]
    labels: list[str]
    label_conflicts: str
    title: str
    body: str


class CopyExecutor(actions.ActionExecutor["CopyAction", "CopyExecutorConfig"]):
    KIND: duplicate_pull.KindT = "copy"
    KIND_PLURAL = "copies"
    HOOK_EVENT_NAME: typing.Literal["action.backport", "action.copy"] = "action.copy"
    BRANCH_PREFIX: str = "copy"
    SUCCESS_MESSAGE: str = "Pull request copies have been created"
    FAILURE_MESSAGE: str = "No copy have been created"

    @property
    def silenced_conclusion(self) -> tuple[check_api.Conclusion, ...]:
        return ()

    @classmethod
    async def create(
        cls,
        action: "CopyAction",
        ctxt: "context.Context",
        rule: "rules.EvaluatedPullRequestRule",
    ) -> "CopyExecutor":
        try:
            bot_account = await action_utils.render_bot_account(
                ctxt,
                action.config["bot_account"],
                required_feature=subscription.Features.BOT_ACCOUNT,
                missing_feature_message=f"Cannot use `bot_account` with {cls.KIND.capitalize()} action",
                required_permissions=[],
            )
        except action_utils.RenderBotAccountFailure as e:
            raise rules.InvalidPullRequestRule(e.title, e.reason)

        github_user: user_tokens.UserTokensUser | None = None
        if bot_account:
            tokens = await ctxt.repository.installation.get_user_tokens()
            github_user = tokens.get_token_for(bot_account)
            if not github_user:
                raise rules.InvalidPullRequestRule(
                    f"Unable to {cls.KIND}: user `{bot_account}` is unknown. ",
                    f"Please make sure `{bot_account}` has logged in Mergify dashboard.",
                )

        try:
            await ctxt.pull_request.render_template(
                action.config["title"], extra_variables=DUPLICATE_TITLE_EXTRA_VARIABLES
            )
        except context.RenderTemplateFailure as rmf:
            # can't occur, template have been checked earlier
            raise rules.InvalidPullRequestRule(
                "Invalid title message",
                str(rmf),
            )

        try:
            await ctxt.pull_request.render_template(
                action.config["body"], extra_variables=DUPLICATE_BODY_EXTRA_VARIABLES
            )
        except context.RenderTemplateFailure as rmf:
            # can't occur, template have been checked earlier
            raise rules.InvalidPullRequestRule(
                "Invalid body message",
                str(rmf),
            )

        branches: list[github_types.GitHubRefType] = action.config["branches"].copy()
        if action.config["regexes"]:
            branches.extend(
                [
                    branch["name"]
                    async for branch in typing.cast(
                        abc.AsyncGenerator[github_types.GitHubBranch, None],
                        ctxt.client.items(
                            f"{ctxt.base_url}/branches",
                            resource_name="branches",
                            page_limit=10,
                        ),
                    )
                    if any(
                        regex.match(branch["name"])
                        for regex in action.config["regexes"]
                    )
                ]
            )

        assignees = [
            user
            for user in await action_utils.render_users_template(
                ctxt, action.config["assignees"]
            )
            if not user.endswith("[bot]")
        ]

        return cls(
            ctxt,
            rule,
            CopyExecutorConfig(
                {
                    "bot_account": github_user,
                    "branches": branches,
                    "ignore_conflicts": action.config["ignore_conflicts"],
                    "assignees": assignees,
                    "labels": action.config["labels"],
                    "label_conflicts": action.config["label_conflicts"],
                    "title": action.config["title"],
                    "body": action.config["body"],
                }
            ),
        )

    async def _copy(
        self,
        branch_name: github_types.GitHubRefType,
    ) -> tuple[check_api.Conclusion, str]:
        """Copy the PR to a branch.

        Returns a tuple of strings (state, reason).
        """

        # NOTE(sileht): Ensure branch exists first
        escaped_branch_name = parse.quote(branch_name, safe="")
        try:
            await self.ctxt.client.item(
                f"{self.ctxt.base_url}/branches/{escaped_branch_name}"
            )
        except http.HTTPStatusError as e:
            detail = f"{self.KIND.capitalize()} to branch `{branch_name}` failed: "
            if e.response.status_code >= 500:
                state = check_api.Conclusion.PENDING
            else:
                state = check_api.Conclusion.FAILURE
                detail += e.response.json()["message"]
            return state, detail

        # NOTE(sileht) does the duplicate have already been done ?
        new_pull = await self.get_existing_duplicate_pull(branch_name)

        # No, then do it
        if not new_pull:
            try:
                users_to_add = [
                    user
                    for user in await action_utils.render_users_template(
                        self.ctxt, self.config["assignees"]
                    )
                    if not user.endswith("[bot]")
                ]
                pull_duplicate = await duplicate_pull.duplicate(
                    self.ctxt,
                    branch_name,
                    title_template=self.config["title"],
                    body_template=self.config["body"],
                    on_behalf=self.config["bot_account"],
                    labels=self.config["labels"],
                    label_conflicts=self.config["label_conflicts"],
                    ignore_conflicts=self.config["ignore_conflicts"],
                    assignees=users_to_add,
                    branch_prefix=self.BRANCH_PREFIX,
                )
                if pull_duplicate is not None:
                    new_pull = pull_duplicate.pull
                    await signals.send(
                        self.ctxt.repository,
                        self.ctxt.pull["number"],
                        self.HOOK_EVENT_NAME,
                        signals.EventCopyMetadata(
                            {
                                "to": branch_name,
                                "pull_request_number": new_pull["number"],
                                "conflicts": pull_duplicate.conflicts,
                            }
                        ),
                        self.rule.get_signal_trigger(),
                    )

            except duplicate_pull.DuplicateAlreadyExists:
                new_pull = await self.get_existing_duplicate_pull(branch_name)
            except duplicate_pull.DuplicateWithMergeFailure:
                return (
                    check_api.Conclusion.FAILURE,
                    f"{self.KIND.capitalize()} to branch `{branch_name}` failed\nPull request {self.KIND_PLURAL} with merge commits are not supported",
                )

            except duplicate_pull.DuplicateFailed as e:
                return (
                    check_api.Conclusion.FAILURE,
                    f"{self.KIND.capitalize()} to branch `{branch_name}` failed\n{e.reason}",
                )
            except duplicate_pull.DuplicateNotNeeded:
                return (
                    check_api.Conclusion.SUCCESS,
                    f"{self.KIND.capitalize()} to branch `{branch_name}` not needed, change already in branch `{branch_name}`",
                )
            except duplicate_pull.DuplicateUnexpectedError as e:
                self.ctxt.log.error(
                    "duplicate failed",
                    reason=e.reason,
                    branch=branch_name,
                    kind=self.KIND,
                    exc_info=True,
                )
                return (
                    check_api.Conclusion.FAILURE,
                    f"{self.KIND.capitalize()} to branch `{branch_name}` failed: {e.reason}",
                )

        if new_pull:
            return (
                check_api.Conclusion.SUCCESS,
                f"[#{new_pull['number']} {new_pull['title']}]({new_pull['html_url']}) "
                f"has been created for branch `{branch_name}`",
            )

        # TODO(sileht): Should be safe to replace that by a RuntimeError now.
        # Just wait a couple of weeks (2020-03-03)
        self.ctxt.log.error(
            "unexpected %s to branch `%s` failure, "
            "duplicate() report pull copy already exists but it doesn't",
            self.KIND.capitalize(),
            branch_name,
        )

        return (
            check_api.Conclusion.FAILURE,
            f"{self.KIND.capitalize()} to branch `{branch_name}` failed",
        )

    async def run(self) -> check_api.Result:
        if len(self.config["branches"]) == 0:
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                self.FAILURE_MESSAGE,
                "No destination branches found",
            )

        if (
            not self.ctxt.can_change_github_workflow()
            and await self.ctxt.github_workflow_changed()
        ):
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                self.FAILURE_MESSAGE,
                "The new Mergify permissions must be accepted to create pull request with `.github/workflows` changes.\n"
                "You can accept them at https://dashboard.mergify.com/",
            )

        results = [
            await self._copy(branch_name) for branch_name in self.config["branches"]
        ]

        # Pick the first status as the final_status
        conclusion = results[0][0]
        for r in results[1:]:
            if r[0] == check_api.Conclusion.FAILURE:
                conclusion = check_api.Conclusion.FAILURE
                # If we have a failure, everything is set to fail
                break
            elif r[0] == check_api.Conclusion.SUCCESS:
                # If it was None, replace with success
                # Keep checking for a failure just in case
                conclusion = check_api.Conclusion.SUCCESS

        if conclusion == check_api.Conclusion.SUCCESS:
            message = self.SUCCESS_MESSAGE
        elif conclusion == check_api.Conclusion.FAILURE:
            message = self.FAILURE_MESSAGE
        else:
            message = "Pending"

        return check_api.Result(
            conclusion,
            message,
            "\n".join(f"* {detail}" for detail in (r[1] for r in results)),
        )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT

    async def get_existing_duplicate_pull(
        self, branch_name: github_types.GitHubRefType
    ) -> github_types.GitHubPullRequest | None:
        bp_branch = duplicate_pull.get_destination_branch_name(
            self.ctxt.pull["number"], branch_name, self.BRANCH_PREFIX
        )
        pulls = [
            pull
            async for pull in typing.cast(
                abc.AsyncGenerator[github_types.GitHubPullRequest, None],
                self.ctxt.client.items(
                    f"{self.ctxt.base_url}/pulls",
                    resource_name="pulls",
                    page_limit=10,
                    params={
                        "base": branch_name,
                        "sort": "created",
                        "state": "all",
                        "head": f"{self.ctxt.pull['base']['user']['login']}:{bp_branch}",
                    },
                ),
            )
        ]

        return pulls[-1] if pulls else None


class CopyAction(actions.Action):
    flags = actions.ActionFlag.ALLOW_ON_CONFIGURATION_CHANGED
    executor_class = CopyExecutor

    default_restrictions: typing.ClassVar[list[typing.Any]] = [
        "sender-permission>=write"
    ]

    KIND: duplicate_pull.KindT = "copy"

    @property
    def validator(self) -> actions.ValidatorT:
        return {
            voluptuous.Required("bot_account", default=None): voluptuous.Any(
                None, types.Jinja2
            ),
            voluptuous.Required("branches", default=list): [str],
            voluptuous.Required("regexes", default=list): [voluptuous.Coerce(Regex)],
            voluptuous.Required("ignore_conflicts", default=True): bool,
            voluptuous.Required("assignees", default=list): [types.Jinja2],
            voluptuous.Required("labels", default=list): [str],
            voluptuous.Required("label_conflicts", default="conflicts"): str,
            voluptuous.Required(
                "title", default=f"{{{{ title }}}} ({self.KIND} #{{{{ number }}}})"
            ): DuplicateTitleJinja2,
            voluptuous.Required(
                "body",
                default=f"This is an automatic {self.KIND} of pull request #{{{{number}}}} done by [Mergify](https://mergify.com).\n{{{{ cherry_pick_error }}}}"
                + "\n\n---\n\n"
                + constants.MERGIFY_PULL_REQUEST_DOC,
            ): DuplicateBodyJinja2,
        }

    @staticmethod
    def command_to_config(string: str) -> dict[str, typing.Any]:
        if string:
            return {"branches": string.split(" ")}
        else:
            return {}
