import dataclasses
import functools
import typing

import tenacity

from mergify_engine import condition_value_querier
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import gitter
from mergify_engine import redis_utils
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.models.github import user as github_user


if typing.TYPE_CHECKING:
    import logging


@dataclasses.dataclass
class DuplicateAlreadyExists(Exception):
    reason: str


@dataclasses.dataclass
class DuplicateNeedRetry(exceptions.EngineNeedRetry):
    pass


@dataclasses.dataclass
class DuplicateNotNeeded(Exception):
    reason: str


@dataclasses.dataclass
class DuplicateFailed(Exception):
    reason: str


@dataclasses.dataclass
class DuplicateUnexpectedError(DuplicateFailed):
    reason: str


@dataclasses.dataclass
class DuplicateWithMergeFailure(DuplicateFailed):
    reason: str = "merge commits are not supported"


@dataclasses.dataclass
class DuplicateFailedConflicts(DuplicateFailed):
    reason: str


@dataclasses.dataclass
class DuplicateBranchResult:
    target_branch: github_types.GitHubRefType
    destination_branch: github_types.GitHubRefType
    cherry_pick_error: str


GIT_MESSAGE_TO_EXCEPTION = {
    "(non-fast-forward)": DuplicateAlreadyExists,
    "Updates were rejected because the tip of your current branch is behind": DuplicateNeedRetry,
    "Aborting commit due to empty commit message": DuplicateNotNeeded,
    "reference already exists": DuplicateAlreadyExists,
    "You may want to first integrate the remote changes": DuplicateAlreadyExists,
    "is a merge but no -m option was given": DuplicateWithMergeFailure,
    "is not a commit and a branch": DuplicateFailed,
    "couldn't find remote ref": DuplicateFailed,
    "does not have a commit checked": DuplicateFailed,
    "Merge conflict in .gitmodules": DuplicateFailedConflicts,
    "Protected branch update failed for": DuplicateFailed,
    "Failed to merge submodule": DuplicateFailed,
}


@functools.total_ordering
class CommitOrderingKey:
    def __init__(self, obj: github_types.CachedGitHubBranchCommit) -> None:
        self.obj = obj

    @staticmethod
    def order_commit(
        c1: github_types.CachedGitHubBranchCommit,
        c2: github_types.CachedGitHubBranchCommit,
    ) -> int:
        if c1.sha == c2.sha:
            return 0

        for p_sha in c1.parents:
            if c2.sha == p_sha:
                return 1

        return -1

    def __lt__(self, other: "CommitOrderingKey") -> bool:
        return (
            isinstance(other, CommitOrderingKey)
            and self.order_commit(self.obj, other.obj) < 0
        )

    def __eq__(self, other: typing.Any) -> bool:
        return (
            isinstance(other, CommitOrderingKey)
            and self.order_commit(self.obj, other.obj) == 0
        )


def is_base_branch_merge_commit(
    commit: github_types.CachedGitHubBranchCommit,
    base_branch: github_types.GitHubRefType,
) -> bool:
    return (
        commit.commit_message.startswith(f"Merge branch '{base_branch}'")
        and len(commit.parents) == 2
    )


async def _get_commits_without_base_branch_merge(
    ctxt: context.Context,
) -> list[github_types.CachedGitHubBranchCommit]:
    base_branch = ctxt.pull["base"]["ref"]
    return list(
        filter(
            lambda c: not is_base_branch_merge_commit(c, base_branch),
            sorted(await ctxt.commits, key=CommitOrderingKey),
        ),
    )


async def get_commits_to_cherrypick(
    ctxt: context.Context,
) -> list[github_types.CachedGitHubBranchCommit]:
    merge_commit = github_types.to_cached_github_branch_commit(
        typing.cast(
            github_types.GitHubBranchCommit,
            await ctxt.client.item(
                f"{ctxt.base_url}/commits/{ctxt.pull['merge_commit_sha']}",
            ),
        ),
    )

    if len(merge_commit.parents) == 1:
        # NOTE(sileht): We have a rebase+merge or squash+merge
        # We pick all commits until a sha is not linked with our PR

        out_commits: list[github_types.CachedGitHubBranchCommit] = []
        commit = merge_commit
        while True:
            if len(commit.parents) != 1:
                # NOTE(sileht): What is that? A merge here?
                ctxt.log.error("unhandled commit structure")
                return []

            out_commits.insert(0, commit)
            parent_commit_sha = commit.parents[0]

            pull_numbers = [
                p["number"]
                async for p in ctxt.client.items(
                    f"{ctxt.base_url}/commits/{parent_commit_sha}/pulls",
                    resource_name="pulls associated to a commit",
                    page_limit=5,
                    api_version="groot",
                )
                if (
                    p["base"]["repo"]["full_name"]
                    == ctxt.pull["base"]["repo"]["full_name"]
                )
            ]

            # Head repo can be None if deleted in the meantime
            if ctxt.pull["head"]["repo"] is not None:
                pull_numbers += [
                    p["number"]
                    async for p in ctxt.client.items(
                        f"/repos/{ctxt.pull['head']['repo']['full_name']}/commits/{parent_commit_sha}/pulls",
                        api_version="groot",
                        resource_name="pulls associated to a commit",
                        page_limit=5,
                    )
                    if (
                        p["base"]["repo"]["full_name"]
                        == ctxt.pull["base"]["repo"]["full_name"]
                    )
                ]

            if ctxt.pull["number"] not in pull_numbers:
                if len(out_commits) == 1:
                    ctxt.log.debug(
                        "Pull requests merged with one commit rebased, or squashed",
                    )
                else:
                    ctxt.log.debug("Pull requests merged after rebase")
                return out_commits

            # Prepare next iteration
            commit = github_types.to_cached_github_branch_commit(
                typing.cast(
                    github_types.GitHubBranchCommit,
                    await ctxt.client.item(
                        f"{ctxt.base_url}/commits/{parent_commit_sha}",
                    ),
                ),
            )

    elif len(merge_commit.parents) == 2:
        ctxt.log.debug("Pull request merged with merge commit")
        return await _get_commits_without_base_branch_merge(ctxt)

    elif len(merge_commit.parents) >= 3:
        raise DuplicateFailed("merge commit with more than 2 parents are unsupported")
    else:
        raise RuntimeError("merge commit with no parents")


KindT = typing.Literal["backport", "copy"]


def get_destination_branch_name(
    pull_number: github_types.GitHubPullRequestNumber,
    branch_name: github_types.GitHubRefType,
    branch_prefix: str,
) -> github_types.GitHubRefType:
    return github_types.GitHubRefType(
        f"mergify/{branch_prefix}/{branch_name}/pr-{pull_number}",
    )


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=0.2),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception_type(DuplicateNeedRetry),
    reraise=True,
)
async def prepare_branch(
    redis: redis_utils.RedisCache,
    logger: "logging.LoggerAdapter[logging.Logger]",
    pull: github_types.GitHubPullRequest,
    auth: github.GitHubAppInstallationAuth | github.GitHubTokenAuth,
    branch_name: github_types.GitHubRefType,
    branch_prefix: str,
    commits_to_cherry_pick: list[github_types.CachedGitHubBranchCommit],
    *,
    ignore_conflicts: bool = False,
    on_behalf: github_user.GitHubUser | None,
) -> DuplicateBranchResult:
    """Duplicate a pull request.

    :param redis: a redis client
    :param logger: a logger
    :param pull: The pull request.
    :param auth: The httpx auth object to get the installation access token
    :param branch_name: The branch to copy to.
    :param branch_prefix: the prefix of the temporary created branch
    :param commits_to_cherry_pick: The list of commits to cherry-pick
    :param ignore_conflicts: Whether to commit the result if the cherry-pick fails.
    :param on_behalf: The user to impersonate
    """
    destination_branch = get_destination_branch_name(
        pull["number"],
        branch_name,
        branch_prefix,
    )

    cherry_pick_error: str = ""

    # TODO(sileht): This can be done with the GitHub API only I think:
    # An example:
    # https://github.com/shiqiyang-okta/ghpick/blob/master/ghpick/cherry.py
    async with gitter.Gitter(logger) as git:
        try:
            if on_behalf is None:
                async with github.AsyncGitHubInstallationClient(auth) as client:
                    token = await client.get_access_token()
                await git.configure(redis)
                username = "x-access-token"
                password = token
            else:
                await git.configure(redis, on_behalf)
                username = on_behalf.oauth_access_token
                password = ""  # nosec

            await git.setup_remote("origin", pull["base"]["repo"], username, password)

            await git.fetch("origin", f"pull/{pull['number']}/head")
            await git.fetch("origin", pull["base"]["ref"])
            await git.fetch("origin", branch_name)
            await git(
                "checkout",
                "--quiet",
                "-b",
                destination_branch,
                f"origin/{branch_name}",
            )

            for commit in commits_to_cherry_pick:
                # FIXME(sileht): GitHub does not allow to fetch only one commit
                # So we have to fetch the branch since the commit date ...
                # git("fetch", "origin", "%s:refs/remotes/origin/%s-commit" %
                #    (commit["sha"], commit["sha"])
                #    )
                # last_commit_date = commit["commit"]["committer"]["date"]
                # git("fetch", "origin", ctxt.pull["base"]["ref"],
                #    "--shallow-since='%s'" % last_commit_date)
                try:
                    await git("cherry-pick", "-x", commit.sha)
                except (
                    gitter.GitAuthenticationFailure,
                    gitter.GitErrorRetriable,
                    gitter.GitFatalError,
                ):
                    raise
                except gitter.GitError as e:  # pragma: no cover
                    if "The previous cherry-pick is now empty," in e.output:
                        await git("cherry-pick", "--skip")
                        continue

                    for message in GIT_MESSAGE_TO_EXCEPTION.keys():
                        if message in e.output:
                            raise

                    logger.info("fail to cherry-pick %s: %s", commit.sha, e.output)
                    output = await git("status")
                    cherry_pick_error += f"Cherry-pick of {commit.sha} has failed:\n```\n{output}```\n\n\n"
                    if not ignore_conflicts:
                        raise DuplicateFailedConflicts(cherry_pick_error)
                    await git("add", "*", _env={"GIT_NOGLOB_PATHSPECS": "0"})
                    await git("commit", "-a", "--no-edit", "--allow-empty")

            await git("push", "origin", destination_branch)
        except gitter.GitMergifyNamespaceConflict as e:
            raise DuplicateFailed(
                "`Mergify uses `mergify/...` namespace for creating temporary branches. "
                "A branch of your repository is conflicting with this namespace\n"
                f"```\n{e.output}\n```\n",
            )
        except gitter.GitAuthenticationFailure as e:
            if on_behalf is None:
                # Need to get a new token
                raise DuplicateNeedRetry(
                    f"Git reported the following error:\n```\n{e.output}\n```\n",
                )
            raise DuplicateUnexpectedError(
                f"Git reported the following error:\n```\n{e.output}\n```\n",
            )
        except gitter.GitErrorRetriable as e:
            raise DuplicateNeedRetry(
                f"Git reported the following error:\n```\n{e.output}\n```\n",
            )
        except gitter.GitFatalError as e:
            raise DuplicateFailed(
                f"Git reported the following error:\n```\n{e.output}\n```\n",
            )
        except gitter.GitError as e:  # pragma: no cover
            for message, out_exception in GIT_MESSAGE_TO_EXCEPTION.items():
                if message in e.output:
                    raise out_exception(
                        f"Git reported the following error:\n```\n{e.output}\n```\n",
                    )
            logger.error(
                "duplicate pull failed",
                output=e.output,
                returncode=e.returncode,
                exc_info=True,
            )
            raise DuplicateUnexpectedError(e.output)

    if cherry_pick_error:
        cherry_pick_error += (
            "To fix up this pull request, you can check it out locally. "
            "See documentation: "
            "https://docs.github.com/en/github/"
            "collaborating-with-pull-requests/reviewing-changes-in-pull-requests/checking-out-pull-requests-locally"
        )

    return DuplicateBranchResult(branch_name, destination_branch, cherry_pick_error)


async def create_duplicate_pull(
    ctxt: context.Context,
    duplicate_branch_result: DuplicateBranchResult,
    title_template: str,
    body_template: str,
    on_behalf: github_user.GitHubUser | None = None,
    labels: list[str] | None = None,
    label_conflicts: str | None = None,
    assignees: list[str] | None = None,
) -> github_types.GitHubPullRequest:
    """Create a pull request.

    :param ctxt: The pull request.
    :type ctxt: py:class:mergify_engine.context.Context

    :param duplicate_branch_result: The result object of prepare_branch()
    :param title_template: The pull request title template.
    :param body_template: The pull request body template.
    :param on_behalf: The user to impersonate
    :param labels: The list of labels to add to the created PR.
    :param label_conflicts: The label to add to the created PR when cherry-pick failed.
    :param assignees: The list of users to be assigned to the created PR.
    """

    pull_attrs = condition_value_querier.PullRequest(ctxt)
    try:
        title = await pull_attrs.render_template(
            title_template,
            extra_variables={
                "destination_branch": duplicate_branch_result.target_branch,
            },
        )
    except condition_value_querier.RenderTemplateFailure as rmf:
        raise DuplicateFailed(f"Invalid title message: {rmf}")

    try:
        body_without_error = await pull_attrs.render_template(
            body_template,
            extra_variables={
                "destination_branch": duplicate_branch_result.target_branch,
                "cherry_pick_error": "",
            },
            mandatory_template_variables={
                "cherry_pick_error": "\n{{ cherry_pick_error }}",
            },
        )
        cherry_pick_error_truncated = utils.unicode_truncate(
            duplicate_branch_result.cherry_pick_error,
            constants.GITHUB_PULL_REQUEST_BODY_MAX_SIZE
            - len(body_without_error.encode()),
            "\n(…)\n",
            position="middle",
        )
        body = await pull_attrs.render_template(
            body_template,
            extra_variables={
                "destination_branch": duplicate_branch_result.target_branch,
                "cherry_pick_error": cherry_pick_error_truncated,
            },
            mandatory_template_variables={
                "cherry_pick_error": "\n{{ cherry_pick_error }}",
            },
        )
    except condition_value_querier.RenderTemplateFailure as rmf:
        raise DuplicateFailed(f"Invalid title message: {rmf}")

    try:
        duplicate_pr = typing.cast(
            github_types.GitHubPullRequest,
            (
                await ctxt.client.post(
                    f"{ctxt.base_url}/pulls",
                    json={
                        "title": title,
                        "body": body,
                        "base": duplicate_branch_result.target_branch,
                        "head": duplicate_branch_result.destination_branch,
                    },
                    oauth_token=on_behalf.oauth_access_token if on_behalf else None,
                )
            ).json(),
        )
    except http.HTTPClientSideError as e:
        if e.status_code == 422:
            if "No commits between" in e.message:
                if duplicate_branch_result.cherry_pick_error:
                    raise DuplicateFailed(duplicate_branch_result.cherry_pick_error)
                raise DuplicateNotNeeded(e.message)

            if "A pull request already exists" in e.message:
                raise DuplicateAlreadyExists(e.message)

        raise

    effective_labels = []
    if labels is not None:
        effective_labels.extend(labels)

    if duplicate_branch_result.cherry_pick_error:
        if label_conflicts is not None:
            effective_labels.append(label_conflicts)

    if len(effective_labels) > 0:
        await ctxt.client.post(
            f"{ctxt.base_url}/issues/{duplicate_pr['number']}/labels",
            json={"labels": effective_labels},
        )

    if assignees is not None and len(assignees) > 0:
        # NOTE(sileht): we don't have to deal with invalid assignees as GitHub
        # just ignore them and always return 201
        await ctxt.client.post(
            f"{ctxt.base_url}/issues/{duplicate_pr['number']}/assignees",
            json={"assignees": assignees},
        )

    return duplicate_pr
