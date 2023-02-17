import base64
from collections import abc
import datetime
import functools
import re
import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import date
from mergify_engine import delayed_refresh
from mergify_engine import duplicate_pull
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import refresher
from mergify_engine import signals
from mergify_engine.actions import utils as action_utils
from mergify_engine.clients import http
from mergify_engine.dashboard import subscription
from mergify_engine.dashboard import user_tokens
from mergify_engine.rules import types
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.worker import gitter_service


def Regex(value: str) -> re.Pattern[str]:
    try:
        return re.compile(value)
    except re.error as e:
        raise voluptuous.Invalid(str(e))


COPY_STATE_EXPIRATION = datetime.timedelta(days=7)

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


class CopyResult(typing.NamedTuple):
    branch: github_types.GitHubRefType
    status: check_api.Conclusion
    details: str
    job_id: gitter_service.GitterJobId | None


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
        rule: "prr_config.EvaluatedPullRequestRule",
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
            raise prr_config.InvalidPullRequestRule(e.title, e.reason)

        github_user: user_tokens.UserTokensUser | None = None
        if bot_account:
            tokens = await ctxt.repository.installation.get_user_tokens()
            github_user = tokens.get_token_for(bot_account)
            if not github_user:
                raise prr_config.InvalidPullRequestRule(
                    f"Unable to {cls.KIND}: user `{bot_account}` is unknown. ",
                    f"Please make sure `{bot_account}` has logged in Mergify dashboard.",
                )

        try:
            await ctxt.pull_request.render_template(
                action.config["title"], extra_variables=DUPLICATE_TITLE_EXTRA_VARIABLES
            )
        except context.RenderTemplateFailure as rmf:
            # can't occur, template have been checked earlier
            raise prr_config.InvalidPullRequestRule(
                "Invalid title message",
                str(rmf),
            )

        try:
            await ctxt.pull_request.render_template(
                action.config["body"], extra_variables=DUPLICATE_BODY_EXTRA_VARIABLES
            )
        except context.RenderTemplateFailure as rmf:
            # can't occur, template have been checked earlier
            raise prr_config.InvalidPullRequestRule(
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
        job_id: gitter_service.GitterJobId | None,
    ) -> CopyResult:
        """Copy the PR to a branch.

        Returns a tuple of strings (state, reason).
        """

        # NOTE(sileht) does the duplicate have already been done ?
        new_pull = await self.get_existing_duplicate_pull(branch_name)
        if new_pull is not None:
            return self._get_success_copy_result(branch_name, new_pull)

        job = await self._get_job(job_id)
        if job is None:
            try:
                # NOTE(sileht): Ensure branch exists first
                await self.ctxt.repository.get_branch(branch_name)
            except http.HTTPClientSideError as e:
                return self._get_failure_copy_result(
                    branch_name, f"GitHub error: ```{e.response.json()['message']}```"
                )

            try:
                commits = await duplicate_pull.get_commits_to_cherrypick(self.ctxt)
            except duplicate_pull.DuplicateFailed as e:
                return self._get_failure_copy_result(branch_name, e.reason)

            job = await self._create_job(branch_name, commits)

        # check if job has finished
        if job.task is None or not job.task.done():
            return self._get_inprogress_copy_result(branch_name, job.id)

        assignees = await self._get_assignees()
        try:
            duplicate_branch_result = job.result()
            new_pull = await duplicate_pull.create_duplicate_pull(
                self.ctxt,
                duplicate_branch_result,
                title_template=self.config["title"],
                body_template=self.config["body"],
                on_behalf=self.config["bot_account"],
                labels=self.config["labels"],
                label_conflicts=self.config["label_conflicts"],
                assignees=assignees,
            )
        except duplicate_pull.DuplicateAlreadyExists:
            new_pull = await self.get_existing_duplicate_pull(branch_name)
            if new_pull is None:
                return self._get_failure_copy_result(branch_name, "")
            else:
                return self._get_success_copy_result(branch_name, new_pull)
        except duplicate_pull.DuplicateNotNeeded:
            return CopyResult(
                branch_name,
                check_api.Conclusion.SUCCESS,
                f"{self.KIND.capitalize()} to branch `{branch_name}` not needed, change already in branch `{branch_name}`",
                None,
            )
        except duplicate_pull.DuplicateFailed as e:
            if isinstance(e, duplicate_pull.DuplicateUnexpectedError):
                self.ctxt.log.error(
                    "duplicate failed",
                    reason=e.reason,
                    branch=branch_name,
                    kind=self.KIND,
                    exc_info=True,
                )
            return self._get_failure_copy_result(branch_name, e.reason)

        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            self.HOOK_EVENT_NAME,
            signals.EventCopyMetadata(
                {
                    "to": branch_name,
                    "pull_request_number": new_pull["number"],
                    "conflicts": bool(duplicate_branch_result.cherry_pick_error),
                }
            ),
            self.rule.get_signal_trigger(),
        )
        return self._get_success_copy_result(branch_name, new_pull)

    @classmethod
    def _get_inprogress_copy_result(
        cls,
        branch_name: github_types.GitHubRefType,
        job_id: gitter_service.GitterJobId | None,
    ) -> CopyResult:
        message = f"{cls.KIND.capitalize()} to branch `{branch_name}` in progress"
        return CopyResult(branch_name, check_api.Conclusion.PENDING, message, job_id)

    @classmethod
    def _get_failure_copy_result(
        cls, branch_name: github_types.GitHubRefType, details: str
    ) -> CopyResult:
        message = f"{cls.KIND.capitalize()} to branch `{branch_name}` failed"
        if details:
            message += f"\n\n{details}"
        return CopyResult(branch_name, check_api.Conclusion.FAILURE, message, None)

    @staticmethod
    def _get_success_copy_result(
        branch_name: github_types.GitHubRefType,
        new_pull: github_types.GitHubPullRequest,
    ) -> CopyResult:
        return CopyResult(
            branch_name,
            check_api.Conclusion.SUCCESS,
            f"[#{new_pull['number']} {new_pull['title']}]({new_pull['html_url']}) "
            f"has been created for branch `{branch_name}`",
            None,
        )

    async def _get_job(
        self,
        job_id: gitter_service.GitterJobId | None,
    ) -> gitter_service.GitterJob[duplicate_pull.DuplicateBranchResult] | None:
        if job_id is None:
            return None
        return typing.cast(
            gitter_service.GitterJob[duplicate_pull.DuplicateBranchResult] | None,
            gitter_service.get_job(job_id),
        )

    async def _create_job(
        self,
        branch_name: github_types.GitHubRefType,
        commits_to_cherry_pick: list[github_types.CachedGitHubBranchCommit],
    ) -> gitter_service.GitterJob[duplicate_pull.DuplicateBranchResult]:
        job = gitter_service.GitterJob[duplicate_pull.DuplicateBranchResult](
            functools.partial(
                duplicate_pull.prepare_branch,
                self.ctxt.repository.installation.redis.cache_bytes,
                self.ctxt.log,
                self.ctxt.pull,
                self.ctxt.client.auth,
                branch_name=branch_name,
                branch_prefix=self.BRANCH_PREFIX,
                commits_to_cherry_pick=commits_to_cherry_pick,
                ignore_conflicts=self.config["ignore_conflicts"],
                on_behalf=self.config["bot_account"],
            ),
            functools.partial(
                refresher.send_pull_refresh,
                self.ctxt.repository.installation.redis.stream,
                self.ctxt.repository.repo,
                action="internal",
                pull_request_number=self.ctxt.pull["number"],
                source=f"internal/{self.KIND}",
            ),
        )
        gitter_service.send_job(job)
        return job

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

        results = await self._do_copies()

        # Pick the first status as the final_status
        conclusion = results[0].status
        for r in results[1:]:
            if r.status == check_api.Conclusion.FAILURE:
                conclusion = check_api.Conclusion.FAILURE
                # If we have a failure, everything is set to fail
                break
            elif r.status == check_api.Conclusion.SUCCESS:
                # If it was None, replace with success
                # Keep checking for a failure just in case
                conclusion = check_api.Conclusion.SUCCESS

        if conclusion == check_api.Conclusion.SUCCESS:
            message = self.SUCCESS_MESSAGE
        elif conclusion == check_api.Conclusion.FAILURE:
            message = self.FAILURE_MESSAGE
        else:
            message = "Pending"

        if conclusion in (check_api.Conclusion.SUCCESS, check_api.Conclusion.FAILURE):
            await self._clear_state()
        else:
            await self._save_state(results)

        return check_api.Result(
            conclusion,
            message,
            "\n".join(f"* {detail}" for detail in (r.details for r in results)),
        )

    async def _get_assignees(self) -> list[str]:
        return [
            user
            for user in await action_utils.render_users_template(
                self.ctxt, self.config["assignees"]
            )
            if not user.endswith("[bot]")
        ]

    async def _do_copies(self) -> list[CopyResult]:
        previous_results = await self._load_state()

        results: list[CopyResult] = []
        for branch_name in self.config["branches"]:
            previous_result = previous_results.get(
                branch_name,
                CopyResult(branch_name, check_api.Conclusion.PENDING, "", None),
            )
            if previous_result.status == check_api.Conclusion.PENDING:
                try:
                    result = await self._copy(branch_name, previous_result.job_id)
                except http.HTTPServerSideError:
                    await delayed_refresh.plan_refresh_at_least_at(
                        self.ctxt.repository,
                        self.ctxt.pull["number"],
                        at=date.utcnow() + datetime.timedelta(minutes=30),
                    )
                    result = self._get_inprogress_copy_result(
                        branch_name, previous_result.job_id
                    )
            else:
                result = previous_result
            results.append(result)

        return results

    @property
    def _state_redis_key(self) -> str:
        # NOTE(sileht): we use base64 to ensure all chars of the rule name is
        # compatible with Redis allowed chars
        rule_name_encoded = base64.urlsafe_b64encode(self.rule.name.encode()).decode()
        return f"{self.KIND}-state/{self.ctxt.repository.repo['id']}/{self.ctxt.pull['number']}/{rule_name_encoded}"

    async def _clear_state(self) -> None:
        await self.ctxt.repository.installation.redis.cache_bytes.delete(
            self._state_redis_key,
        )

    async def _save_state(self, results: list[CopyResult]) -> None:
        await self.ctxt.repository.installation.redis.cache_bytes.set(
            self._state_redis_key, json.dumps(results), ex=COPY_STATE_EXPIRATION
        )

    async def _load_state(self) -> dict[github_types.GitHubRefType, CopyResult]:
        data = await self.ctxt.repository.installation.redis.cache_bytes.get(
            self._state_redis_key,
        )
        if data is None:
            return {}
        return {result[0]: CopyResult(*result) for result in json.loads(data)}

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        # NOTE(sileht): In case we cancel the action on a half created backport, it's too risky
        # to cancel it and cleanup created resources. People may have rules that
        # invalid the condition on purpose after the backport has been created. We
        # should keep the same behavior as before, keep it.
        # So here we ensure we finish it in such case.
        if await self._load_state():
            return await self.run()
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
