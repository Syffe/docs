from collections import abc
import re
import typing

from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import refresher
from mergify_engine import rules
from mergify_engine import worker_pusher
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.dashboard import user_tokens


REQUIRED_STATUS_RE = re.compile(r'Required status check "([^"]*)" is expected.')
FORBIDDEN_MERGE_COMMITS_MSG = "Merge commits are not allowed on this repository."
FORBIDDEN_SQUASH_MERGE_MSG = "Squash merges are not allowed on this repository."
FORBIDDEN_REBASE_MERGE_MSG = "Rebase merges are not allowed on this repository."

PendingResultBuilderT = abc.Callable[
    [context.Context, "rules.EvaluatedRule"],
    abc.Awaitable[check_api.Result],
]
MergeMethodT = typing.Literal["merge", "rebase", "squash", "fast-forward"]
RebaseFallbackT = typing.Literal["merge", "squash", "none", None]


class MergeUtilsMixin:
    async def _refresh_for_retry(self, ctxt: context.Context) -> None:
        await refresher.send_pull_refresh(
            ctxt.repository.installation.redis.stream,
            ctxt.pull["base"]["repo"],
            pull_request_number=ctxt.pull["number"],
            action="internal",
            source="merge failed and need to be retried",
            priority=worker_pusher.Priority.immediate,
        )

    async def common_merge(
        self,
        ctxt: context.Context,
        rule: "rules.EvaluatedRule",
        merge_method: MergeMethodT,
        merge_rebase_fallback: RebaseFallbackT,
        merge_bot_account: github_types.GitHubLogin | None,
        commit_message_template: str | None,
        pending_result_builder: PendingResultBuilderT,
    ) -> check_api.Result:
        final_merge_method: MergeMethodT
        if merge_method != "rebase" or ctxt.pull["rebaseable"]:
            final_merge_method = merge_method
        elif merge_rebase_fallback == "merge":
            final_merge_method = "merge"
        elif merge_rebase_fallback == "squash":
            final_merge_method = "squash"
        else:
            if merge_rebase_fallback is None:
                ctxt.log.info("legacy rebase_fallback=null used")
            return check_api.Result(
                check_api.Conclusion.ACTION_REQUIRED,
                "Automatic rebasing is not possible, manual intervention required",
                "",
            )

        data = {}

        github_user: user_tokens.UserTokensUser | None = None
        if merge_bot_account:
            tokens = await ctxt.repository.installation.get_user_tokens()
            github_user = tokens.get_token_for(merge_bot_account)
            if not github_user:
                return check_api.Result(
                    check_api.Conclusion.FAILURE,
                    f"Unable to rebase: user `{merge_bot_account}` is unknown. ",
                    f"Please make sure `{merge_bot_account}` has logged in Mergify dashboard.",
                )

        if final_merge_method == "fast-forward":
            try:
                await ctxt.client.put(
                    f"{ctxt.base_url}/git/refs/heads/{ctxt.pull['base']['ref']}",
                    oauth_token=github_user["oauth_access_token"]
                    if github_user
                    else None,
                    json={"sha": ctxt.pull["head"]["sha"]},
                )
            except http.HTTPClientSideError as e:  # pragma: no cover
                if "Update is not a fast forward" in e.message:
                    ctxt.log.info(
                        "Base branch was modified in the meantime, retrying",
                        status_code=e.status_code,
                        error_message=e.message,
                    )
                    await self._refresh_for_retry(ctxt)
                    return await pending_result_builder(ctxt, rule)

                await ctxt.update()
                if ctxt.pull["merged"]:
                    ctxt.log.info("merged in the meantime")
                else:
                    return await self._handle_merge_error(
                        e, ctxt, rule, pending_result_builder
                    )
            else:
                ctxt.log.info("merged")

            # NOTE(sileht): We can't use merge_report() here, because it takes
            # some time for GitHub to detect this pull request has been
            # merged. Just after the fast-forward git push, mergeable_state is
            # mark as conflict and a bit later as unknown and merged attribute set to
            # true. We can't block here just for this.
            return check_api.Result(
                check_api.Conclusion.SUCCESS,
                "The pull request has been merged",
                f"The pull request has been merged at *{ctxt.pull['head']['sha']}*.",
            )

        else:  # Via API

            try:
                commit_title_and_message = await ctxt.pull_request.get_commit_message(
                    commit_message_template,
                )
            except context.RenderTemplateFailure as rmf:
                return check_api.Result(
                    check_api.Conclusion.ACTION_REQUIRED,
                    "Invalid commit message",
                    str(rmf),
                )

            if commit_title_and_message is not None:
                title, message = commit_title_and_message
                data["commit_title"] = title
                data["commit_message"] = message

            data["sha"] = ctxt.pull["head"]["sha"]
            data["merge_method"] = final_merge_method

            try:
                await ctxt.client.put(
                    f"{ctxt.base_url}/pulls/{ctxt.pull['number']}/merge",
                    oauth_token=github_user["oauth_access_token"]
                    if github_user
                    else None,
                    json=data,
                )
            except http.HTTPClientSideError as e:  # pragma: no cover
                await ctxt.update()
                if ctxt.pull["merged"]:
                    ctxt.log.info("merged in the meantime")
                else:
                    return await self._handle_merge_error(
                        e, ctxt, rule, pending_result_builder
                    )
            else:
                await ctxt.update(wait_merged=True)
                ctxt.log.info("merged")

        result = await self.merge_report(
            ctxt, merge_method, merge_rebase_fallback, merge_bot_account
        )
        if result:
            return result
        else:
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                "Unexpected after merge pull request state",
                "The pull request has been merged while GitHub API still reports it as opened.",
            )

    async def _handle_merge_error(
        self,
        e: http.HTTPClientSideError,
        ctxt: context.Context,
        rule: "rules.EvaluatedRule",
        pending_result_builder: PendingResultBuilderT,
    ) -> check_api.Result:
        if "Head branch was modified" in e.message:
            ctxt.log.info(
                "Head branch was modified in the meantime, retrying",
                status_code=e.status_code,
                error_message=e.message,
            )
            await self._refresh_for_retry(ctxt)
            return await pending_result_builder(ctxt, rule)
        elif "Base branch was modified" in e.message:
            # NOTE(sileht): The base branch was modified between pull.is_behind call and
            # here, usually by something not merged by mergify. So we need sync it again
            # with the base branch.
            ctxt.log.info(
                "Base branch was modified in the meantime, retrying",
                status_code=e.status_code,
                error_message=e.message,
            )
            await self._refresh_for_retry(ctxt)
            return await pending_result_builder(ctxt, rule)

        elif e.status_code == 405:
            if REQUIRED_STATUS_RE.match(e.message):
                # NOTE(sileht): when brand protection are enabled, we might get
                # a 405 with branch protection issue, when the head branch was
                # just updated. So we check if the head sha has changed in
                # meantime to confirm
                new_pull = await ctxt.client.item(
                    f"{ctxt.base_url}/pulls/{ctxt.pull['number']}"
                )
                if new_pull["head"]["sha"] != ctxt.pull["head"]["sha"]:
                    ctxt.log.info(
                        "Head branch was modified in the meantime, retrying",
                        status_code=e.status_code,
                        error_message=e.message,
                    )
                    await self._refresh_for_retry(ctxt)
                    return await pending_result_builder(ctxt, rule)

                ctxt.log.info(
                    "Waiting for the branch protection required status checks to be validated",
                    status_code=e.status_code,
                    error_message=e.message,
                )
                return check_api.Result(
                    check_api.Conclusion.PENDING,
                    "Waiting for the branch protection required status checks to be validated",
                    "[Branch protection](https://docs.github.com/en/github/administering-a-repository/about-protected-branches) is enabled and is preventing Mergify "
                    "to merge the pull request. Mergify will merge when "
                    "the [required status check](https://docs.github.com/en/github/administering-a-repository/about-required-status-checks) "
                    f"validate the pull request. (detail: {e.message})",
                )
            elif FORBIDDEN_REBASE_MERGE_MSG in e.message:
                ctxt.log.info(
                    "Repository configuration doesn't allow rebase merge",
                    status_code=e.status_code,
                    error_message=e.message,
                )
                return check_api.Result(
                    check_api.Conclusion.CANCELLED,
                    e.message,
                    "The repository configuration doesn't allow rebase merge. "
                    "The merge `method` configured in Mergify configuration must be "
                    "allowed in the repository configuration settings.",
                )

            elif FORBIDDEN_SQUASH_MERGE_MSG in e.message:
                ctxt.log.info(
                    "Repository configuration doesn't allow squash merge",
                    status_code=e.status_code,
                    error_message=e.message,
                )
                return check_api.Result(
                    check_api.Conclusion.CANCELLED,
                    e.message,
                    "The repository configuration doesn't allow squash merge. "
                    "The merge `method` configured in Mergify configuration must be "
                    "allowed in the repository configuration settings.",
                )

            elif FORBIDDEN_MERGE_COMMITS_MSG in e.message:
                ctxt.log.info(
                    "Repository configuration doesn't allow merge commit",
                    status_code=e.status_code,
                    error_message=e.message,
                )
                return check_api.Result(
                    check_api.Conclusion.CANCELLED,
                    e.message,
                    "The repository configuration doesn't allow merge commits. "
                    "The merge `method` configured in Mergify configuration must be "
                    "allowed in the repository configuration settings.",
                )

            else:
                ctxt.log.info(
                    "Branch protection settings are not validated anymore",
                    status_code=e.status_code,
                    error_message=e.message,
                )

                return check_api.Result(
                    check_api.Conclusion.CANCELLED,
                    "Branch protection settings are not validated anymore",
                    "[Branch protection](https://docs.github.com/en/github/administering-a-repository/about-protected-branches) is enabled and is preventing Mergify "
                    "to merge the pull request. Mergify will merge when "
                    "branch protection settings validate the pull request once again. "
                    f"(detail: {e.message})",
                )
        else:
            message = "Mergify failed to merge the pull request"
            ctxt.log.info(
                "merge fail",
                status_code=e.status_code,
                mergify_message=message,
                error_message=e.message,
            )
            return check_api.Result(
                check_api.Conclusion.CANCELLED,
                message,
                f"GitHub error message: `{e.message}`",
            )

    async def merge_report(
        self,
        ctxt: context.Context,
        merge_method: MergeMethodT,
        merge_rebase_fallback: RebaseFallbackT,
        merge_bot_account: github_types.GitHubLogin | None,
    ) -> check_api.Result | None:
        if ctxt.pull["draft"]:
            conclusion = check_api.Conclusion.PENDING
            title = "Draft flag needs to be removed"
            summary = ""
        elif ctxt.pull["merged"]:
            mergify_bot = await github.GitHubAppInfo.get_bot()
            if ctxt.pull["merged_by"] is None:
                mode = "somehow"
            elif (
                ctxt.pull["merged_by"]["id"] == mergify_bot["id"]
                or ctxt.pull["merged_by"]["login"] == merge_bot_account
            ):
                mode = "automatically"
            else:
                mode = "manually"
            conclusion = check_api.Conclusion.SUCCESS
            title = f"The pull request has been merged {mode}"
            summary = f"The pull request has been merged {mode} at *{ctxt.pull['merge_commit_sha']}*"
        elif ctxt.closed:
            conclusion = check_api.Conclusion.CANCELLED
            title = "The pull request has been closed manually"
            summary = ""

        elif (
            await self._is_branch_protection_linear_history_enabled(ctxt)
            and merge_method == "merge"
        ):
            conclusion = check_api.Conclusion.FAILURE
            title = "Branch protection setting 'linear history' conflicts with Mergify configuration"
            summary = "Branch protection setting 'linear history' works only if `method: squash` or `method: rebase`."

        elif (
            not ctxt.can_change_github_workflow()
            and await ctxt.github_workflow_changed()
        ):
            conclusion = check_api.Conclusion.ACTION_REQUIRED
            title = "Pull request must be merged manually"
            summary = """The new Mergify permissions must be accepted to merge pull request with `.github/workflows` changes.\n
You can accept them at https://dashboard.mergify.com/\n
\n
In the meantime, the pull request must be merged manually."
"""
        elif (
            ctxt.pull["rebaseable"] is False
            and merge_method == "rebase"
            and merge_rebase_fallback in (None, "none")
        ):
            return check_api.Result(
                check_api.Conclusion.ACTION_REQUIRED,
                "Pull request must be rebased manually",
                "The pull request can't be rebased without conflict and must be rebased manually",
            )
        # NOTE(sileht): remaining state "behind, clean, unstable, has_hooks
        # are OK for us
        else:
            return None

        return check_api.Result(conclusion, title, summary)

    @staticmethod
    async def _is_branch_protection_linear_history_enabled(
        ctxt: context.Context,
    ) -> bool:
        protection = await ctxt.repository.get_branch_protection(
            ctxt.pull["base"]["ref"]
        )
        return (
            protection is not None
            and "required_linear_history" in protection
            and protection["required_linear_history"]["enabled"]
        )
