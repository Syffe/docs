from __future__ import annotations

from collections import abc
import datetime
import re
import typing

from mergify_engine import check_api
from mergify_engine import condition_value_querier
from mergify_engine import constants
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import pull_request_getter
from mergify_engine import queue
from mergify_engine import redis_utils
from mergify_engine import refresher
from mergify_engine import worker_pusher
from mergify_engine.actions import utils as action_utils
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.models.github import user as github_user


if typing.TYPE_CHECKING:
    from mergify_engine import context

RECENTLY_MERGED_TRACKER_EXPIRATION = datetime.timedelta(hours=1)
REQUIRED_STATUS_RE = re.compile(r'Required status check "([^"]*)" is expected.')
FORBIDDEN_MERGE_COMMITS_MSG = "Merge commits are not allowed on this repository."
FORBIDDEN_SQUASH_MERGE_MSG = "Squash merges are not allowed on this repository."
FORBIDDEN_REBASE_MERGE_MSG = "Rebase merges are not allowed on this repository."
PULL_REQUEST_IS_NOT_MERGEABLE = "Pull Request is not mergeable"

"""
We've esteemed 7 days is enough for keeping merge_commit_sha history of the pull requests we have merged with Mergify.
It should avoid loosing information or keeping them for too long to make decisions, and also cover weekends
where repository activity is often reduced.
"""
MERGE_COMMIT_SHA_EXPIRATION = int(datetime.timedelta(days=7).total_seconds())
MERGE_METHOD_EXPIRATION = datetime.timedelta(hours=1)

if typing.TYPE_CHECKING:
    PendingResultBuilderT = abc.Callable[
        [context.Context],
        abc.Awaitable[check_api.Result],
    ]
else:
    PendingResultBuilderT = abc.Callable[..., abc.Awaitable[check_api.Result]]

MergeMethodT = typing.Literal["merge", "rebase", "squash", "fast-forward"]
PullMergePayload = dict[str, str]


class MergeUtilsMixin:
    MAX_REFRESH_ATTEMPTS: typing.ClassVar[int | None] = 15

    @classmethod
    async def _refresh_for_retry(
        cls,
        ctxt: context.Context,
        pending_result_builder: PendingResultBuilderT,
        abort_message: str,
        exception: http.HTTPClientSideError | None = None,
    ) -> check_api.Result:
        try:
            await refresher.send_pull_refresh(
                ctxt.repository.installation.redis.stream,
                ctxt.pull["base"]["repo"],
                pull_request_number=ctxt.pull["number"],
                action="internal",
                source="merge failed and need to be retried",
                priority=worker_pusher.Priority.medium,
                refresh_flag=refresher.RefreshFlag.MERGE_FAILED,
                max_attempts=cls.MAX_REFRESH_ATTEMPTS,
            )
        except refresher.MaxRefreshAttemptsExceeded as e:
            ctxt.log.error(
                "failed to merge after %s refresh attempts",
                e.max_attempts,
                abort_message=abort_message,
                is_conflicting=ctxt.is_conflicting,
                curl=await exception.to_curl() if exception else None,
            )
            return check_api.Result(
                check_api.Conclusion.CANCELLED,
                "Mergify failed to merge the pull request",
                f"GitHub can't merge the pull request after {e.max_attempts} retries.\n{abort_message}",
            )

        ctxt.log.info(
            "%s, retrying",
            abort_message,
            abort_message=abort_message,
            is_conflicting=ctxt.is_conflicting,
            curl=await exception.to_curl() if exception else None,
        )
        return await pending_result_builder(ctxt)

    @staticmethod
    def _get_redis_recently_merged_tracker_key(
        repository_id: github_types.GitHubRepositoryIdType,
        pull_request_number: github_types.GitHubPullRequestNumber,
    ) -> str:
        return f"recently-merged-tracker/{repository_id}/{pull_request_number}"

    @classmethod
    async def has_been_recently_merged(
        cls,
        redis: redis_utils.RedisCache,
        repository_id: github_types.GitHubRepositoryIdType,
        pull_request_number: github_types.GitHubPullRequestNumber,
    ) -> bool:
        recently_merged = await redis.get(
            cls._get_redis_recently_merged_tracker_key(
                repository_id, pull_request_number
            )
        )
        return recently_merged is not None

    @classmethod
    async def create_recently_merged_tracker(
        cls,
        redis: redis_utils.RedisCache,
        repository_id: github_types.GitHubRepositoryIdType,
        pull_request_number: github_types.GitHubPullRequestNumber,
    ) -> None:
        await redis.set(
            cls._get_redis_recently_merged_tracker_key(
                repository_id, pull_request_number
            ),
            date.utcnow().isoformat(),
            ex=RECENTLY_MERGED_TRACKER_EXPIRATION,
        )

    async def common_merge(
        self,
        kind: str,
        ctxt: context.Context,
        merge_method: MergeMethodT | None,
        merge_bot_account: github_types.GitHubLogin | None,
        commit_message_template: str | None,
        pending_result_builder: PendingResultBuilderT,
        branch_protection_injection_mode: queue.BranchProtectionInjectionModeT = "queue",
    ) -> check_api.Result:
        pull_merge_payload = {}

        on_behalf: github_user.GitHubUser | None = None
        if merge_bot_account:
            if branch_protection_injection_mode == "none":
                required_permissions = (
                    github_types.GitHubRepositoryPermission.permissions_above(
                        github_types.GitHubRepositoryPermission.WRITE
                    )
                )
            else:
                # NOTE(sileht): we don't allow admin, because if branch protection are
                # enabled, but not enforced on admins, we may bypass them
                required_permissions = [
                    github_types.GitHubRepositoryPermission.WRITE,
                ]
            try:
                on_behalf = await action_utils.get_github_user_from_bot_account(
                    ctxt.repository,
                    kind,
                    merge_bot_account,
                    required_permissions=required_permissions,
                )
            except action_utils.BotAccountNotFound as e:
                return check_api.Result(e.status, e.title, e.reason)

        if merge_method == "fast-forward":
            try:
                await ctxt.client.put(
                    f"{ctxt.base_url}/git/refs/heads/{ctxt.pull['base']['ref']}",
                    oauth_token=on_behalf.oauth_access_token if on_behalf else None,
                    json={"sha": ctxt.pull["head"]["sha"]},
                )
            except http.HTTPClientSideError as e:  # pragma: no cover
                await ctxt.update()
                if ctxt.pull["merged"]:
                    ctxt.log.info("merged in the meantime")
                else:
                    return await self._handle_merge_error(
                        e, ctxt, pending_result_builder
                    )
            else:
                ctxt.log.info("merged")

            await self.create_recently_merged_tracker(
                ctxt.repository.installation.redis.cache,
                ctxt.repository.repo["id"],
                ctxt.pull["number"],
            )
            # NOTE(sileht): We can't use merge_report() here, because it takes
            # some time for GitHub to detect this pull request has been
            # merged. Just after the fast-forward git push, mergeable_state is
            # mark as conflict and a bit later as unknown and merged attribute set to
            # true. We can't block here just for this.
            return check_api.Result(
                check_api.Conclusion.SUCCESS,
                "The pull request has been merged automatically",
                f"The pull request has been merged automatically at *{ctxt.pull['head']['sha']}*",
            )

        attrs = condition_value_querier.PullRequest(ctxt)
        try:
            commit_title_and_message = await attrs.get_commit_message(
                commit_message_template,
            )
        except condition_value_querier.RenderTemplateFailure as rmf:
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                "Invalid commit message",
                str(rmf),
            )

        if commit_title_and_message is not None:
            title, message = commit_title_and_message
            pull_merge_payload["commit_title"] = title
            pull_merge_payload["commit_message"] = message

        pull_merge_payload["sha"] = ctxt.pull["head"]["sha"]

        try:
            if merge_method is None:
                merge_method = await self._request_merge_without_method(
                    ctxt, pull_merge_payload, on_behalf
                )
            else:
                pull_merge_payload["merge_method"] = merge_method
                await self._request_merge(ctxt, pull_merge_payload, on_behalf)
        except http.HTTPUnauthorized:
            if on_behalf is None:
                raise
            return action_utils.get_invalid_credentials_report(on_behalf)
        except http.HTTPClientSideError as e:  # pragma: no cover
            await ctxt.update()
            if ctxt.pull["merged"]:
                ctxt.log.info("merged in the meantime")
            else:
                return await self._handle_merge_error(e, ctxt, pending_result_builder)
        else:
            await ctxt.update(wait_merged=True, wait_merge_commit_sha=True)
            ctxt.log.info("merged", merge_method=merge_method)

        if ctxt.pull["merge_commit_sha"]:
            await ctxt.repository.installation.redis.queue.set(
                ctxt.redis_merged_by_mergify_key(
                    owner_id=ctxt.repository.installation.owner_id,
                    repo_id=ctxt.repository.repo["id"],
                    merge_commit_sha=ctxt.pull["merge_commit_sha"],
                ),
                ctxt.pull["merge_commit_sha"],
                ex=MERGE_COMMIT_SHA_EXPIRATION,
            )
        else:
            ctxt.log.error("PR got merged with unknown merge_commit_sha")

        await self.create_recently_merged_tracker(
            ctxt.repository.installation.redis.cache,
            ctxt.repository.repo["id"],
            ctxt.pull["number"],
        )
        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            "The pull request has been merged automatically",
            f"The pull request has been merged automatically at *{ctxt.pull['merge_commit_sha']}*",
        )

    async def _request_merge_without_method(
        self,
        ctxt: context.Context,
        pull_merge_payload: PullMergePayload,
        on_behalf: github_user.GitHubUser | None,
    ) -> MergeMethodT:
        methods: list[MergeMethodT] = ["merge", "squash", "rebase"]

        # NOTE(charly): retrieve cached merge method and test it first
        cached_method = await self._get_merge_method(ctxt)
        if cached_method:
            methods.remove(cached_method)
            methods.insert(0, cached_method)

        # NOTE(charly): merge method is not allowed if linear history is
        # required
        if await ctxt.is_branch_protection_linear_history_enabled():
            methods.remove("merge")

        last_client_error: http.HTTPClientSideError | None = None

        for method in methods:
            pull_merge_payload["merge_method"] = method

            try:
                await self._request_merge(ctxt, pull_merge_payload, on_behalf)
            except http.HTTPClientSideError as e:
                # NOTE(charly): if the merge method is not allowed, test the
                # next merge method and save the response in case none works
                # (theoretically impossible)
                if e.status_code == 405 and (
                    FORBIDDEN_MERGE_COMMITS_MSG in e.message
                    or FORBIDDEN_SQUASH_MERGE_MSG in e.message
                    or FORBIDDEN_REBASE_MERGE_MSG in e.message
                ):
                    last_client_error = e
                    continue
                raise
            else:
                # NOTE(charly): new working merge method, store it in the cache
                if method != cached_method:
                    await self._store_merge_method(ctxt, method)
                return method

        # NOTE(charly): no merge method worked, raise the last error
        if last_client_error is not None:
            raise last_client_error

        raise RuntimeError("There should have been a success or a failure")

    async def _get_merge_method(self, ctxt: context.Context) -> MergeMethodT | None:
        cached_method = await ctxt.redis.cache.get(
            self._merge_method_redis_key(
                ctxt.repository.installation.owner_id,
                ctxt.repository.repo["id"],
            ),
        )
        if cached_method is not None:
            return typing.cast(MergeMethodT, cached_method.decode())
        return None

    async def _store_merge_method(
        self, ctxt: context.Context, method: MergeMethodT
    ) -> None:
        await ctxt.redis.cache.set(
            self._merge_method_redis_key(
                ctxt.repository.installation.owner_id,
                ctxt.repository.repo["id"],
            ),
            method,
            ex=MERGE_METHOD_EXPIRATION,
        )

    @staticmethod
    def _merge_method_redis_key(
        owner_id: github_types.GitHubAccountIdType,
        repository_id: github_types.GitHubRepositoryIdType,
    ) -> str:
        return f"merge-method/{owner_id}/{repository_id}"

    async def _request_merge(
        self,
        ctxt: context.Context,
        pull_merge_payload: PullMergePayload,
        on_behalf: github_user.GitHubUser | None,
    ) -> None:
        await ctxt.client.put(
            f"{ctxt.base_url}/pulls/{ctxt.pull['number']}/merge",
            oauth_token=on_behalf.oauth_access_token if on_behalf else None,
            json=pull_merge_payload,
        )

    async def _handle_merge_error(
        self,
        e: http.HTTPClientSideError,
        ctxt: context.Context,
        pending_result_builder: PendingResultBuilderT,
    ) -> check_api.Result:
        if (
            result := await self._handle_merge_error_conditions(
                e, ctxt, pending_result_builder
            )
        ) is not None:
            return result

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

    async def _handle_merge_error_conditions(
        self,
        e: http.HTTPClientSideError,
        ctxt: context.Context,
        pending_result_builder: PendingResultBuilderT,
    ) -> check_api.Result | None:
        if "Head branch was modified" in e.message:
            return await self._refresh_for_retry(
                ctxt,
                pending_result_builder,
                "Head branch was modified in the meantime",
                e,
            )
        if "Head branch is out of date" in e.message:
            return await self._refresh_for_retry(
                ctxt,
                pending_result_builder,
                "Head branch is out of date",
                e,
            )

        if (
            "Update is not a fast forward" in e.message
            or "Base branch was modified" in e.message
        ):
            # NOTE(sileht): The base branch was modified between pull.is_behind call and
            # here, usually by something not merged by mergify. So we need sync it again
            # with the base branch.
            return await self._refresh_for_retry(
                ctxt,
                pending_result_builder,
                "Base branch was modified in the meantime",
                e,
            )

        if e.status_code == 405:
            if REQUIRED_STATUS_RE.match(e.message):
                # NOTE(sileht): when brand protection are enabled, we might get
                # a 405 with branch protection issue, when the head branch was
                # just updated. So we check if the head sha has changed in
                # meantime to confirm
                # Do not use db here, because the pull request we have in db
                # might not be updated yet.
                new_pull = await pull_request_getter.get_pull_request(
                    ctxt.client,
                    ctxt.pull["number"],
                    repo_owner=ctxt.repo_owner_login,
                    repo_name=ctxt.repo_name,
                    force_new=True,
                )
                if new_pull["head"]["sha"] != ctxt.pull["head"]["sha"]:
                    return await self._refresh_for_retry(
                        ctxt,
                        pending_result_builder,
                        "Head branch was modified in the meantime",
                        e,
                    )

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

            if FORBIDDEN_REBASE_MERGE_MSG in e.message:
                ctxt.log.info(
                    "Repository configuration doesn't allow rebase merge",
                    status_code=e.status_code,
                    error_message=e.message,
                )
                return check_api.Result(
                    check_api.Conclusion.CANCELLED,
                    e.message,
                    "The repository configuration doesn't allow rebase merge. "
                    "The merge method configured in Mergify configuration must be "
                    "allowed in the repository configuration settings.",
                )

            if FORBIDDEN_SQUASH_MERGE_MSG in e.message:
                ctxt.log.info(
                    "Repository configuration doesn't allow squash merge",
                    status_code=e.status_code,
                    error_message=e.message,
                )
                return check_api.Result(
                    check_api.Conclusion.CANCELLED,
                    e.message,
                    "The repository configuration doesn't allow squash merge. "
                    "The merge method configured in Mergify configuration must be "
                    "allowed in the repository configuration settings.",
                )

            if FORBIDDEN_MERGE_COMMITS_MSG in e.message:
                ctxt.log.info(
                    "Repository configuration doesn't allow merge commit",
                    status_code=e.status_code,
                    error_message=e.message,
                )
                return check_api.Result(
                    check_api.Conclusion.CANCELLED,
                    e.message,
                    "The repository configuration doesn't allow merge commits. "
                    "The merge method configured in Mergify configuration must be "
                    "allowed in the repository configuration settings.",
                )

            if e.message == PULL_REQUEST_IS_NOT_MERGEABLE:
                # NOTE(charly): set neutral conclusion to be able to enqueue the
                # pull request again
                return check_api.Result(
                    check_api.Conclusion.NEUTRAL,
                    "GitHub can't merge the pull request for now.",
                    "GitHub can't merge the pull request for an unknown reason. "
                    "You should retry later.",
                )

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

        return None

    async def pre_merge_checks(
        self,
        ctxt: context.Context,
        merge_method: MergeMethodT | None,
        merge_bot_account: github_types.GitHubLogin | None,
    ) -> check_api.Result | None:
        if ctxt.pull["merged"]:
            mergify_bot = await github.GitHubAppInfo.get_bot(
                ctxt.repository.installation.redis.cache
            )
            if ctxt.pull["merged_by"] is None:
                mode = "somehow"
                conclusion = check_api.Conclusion.CANCELLED
            elif (
                ctxt.pull["merged_by"]["id"] == mergify_bot["id"]
                or ctxt.pull["merged_by"]["login"] == merge_bot_account
            ):
                if await self.has_been_recently_merged(
                    ctxt.redis.cache,
                    ctxt.repository.repo["id"],
                    ctxt.pull["number"],
                ):
                    mode = "automatically"
                    conclusion = check_api.Conclusion.SUCCESS
                else:
                    mode = "implicitly by merging another pull request"
                    conclusion = check_api.Conclusion.CANCELLED
            else:
                mode = "manually"
                conclusion = check_api.Conclusion.CANCELLED
            title = f"The pull request has been merged {mode}"
            summary = f"The pull request has been merged {mode} at *{ctxt.pull['merge_commit_sha']}*"
        elif ctxt.closed:
            conclusion = check_api.Conclusion.CANCELLED
            title = "The pull request has been closed manually"
            summary = ""
        elif (
            await ctxt.is_branch_protection_linear_history_enabled()
            and merge_method == "merge"
        ):
            conclusion = check_api.Conclusion.FAILURE
            title = "Branch protection setting 'linear history' conflicts with Mergify configuration"
            summary = "Branch protection setting 'linear history' works only if `merge_method: squash` or `merge_method: rebase`."

        elif (
            not ctxt.can_change_github_workflow()
            and await ctxt.github_workflow_changed()
        ):
            conclusion = check_api.Conclusion.FAILURE
            title = "Pull request must be merged manually"
            summary = f"""{constants.NEW_MERGIFY_PERMISSIONS_MUST_BE_ACCEPTED}
\n
In the meantime, the pull request must be merged manually."
"""
        elif ctxt.pull["rebaseable"] is False and merge_method == "rebase":
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                "Pull request must be rebased manually",
                "The pull request can't be rebased without conflict and must be rebased manually",
            )
        # NOTE(sileht): remaining state "behind, clean, unstable, has_hooks
        # are OK for us
        else:
            return None

        return check_api.Result(conclusion, title, summary)
