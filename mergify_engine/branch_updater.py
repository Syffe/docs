from __future__ import annotations

import dataclasses
import logging
import typing

import tenacity

from mergify_engine import constants
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import gitter
from mergify_engine import pull_request_getter
from mergify_engine import settings
from mergify_engine.clients import http


if typing.TYPE_CHECKING:
    from mergify_engine import context
    from mergify_engine.models.github import user as github_user


@dataclasses.dataclass
class BranchUpdateFailure(Exception):
    message: str
    title: str = dataclasses.field(default="Base branch update has failed")


@dataclasses.dataclass
class BranchUpdateNeedRetry(exceptions.EngineNeedRetry):
    pass


GIT_MESSAGE_TO_EXCEPTION = {
    "Could not apply ": BranchUpdateFailure,
    "could not apply ": BranchUpdateFailure,
    "Patch failed at": BranchUpdateFailure,
}


def pre_update_check(ctxt: context.Context) -> None:
    # If PR from a public fork but cannot be edited
    if (
        ctxt.pull_from_fork
        and not ctxt.pull["base"]["repo"]["private"]
        and not ctxt.pull["maintainer_can_modify"]
    ):
        raise BranchUpdateFailure(
            "Mergify needs the author permission to update the base branch of the pull request.\n"
            f"@{ctxt.pull['head']['user']['login']} needs to "
            "[authorize modification on its head branch]"
            "(https://docs.github.com/en/github/collaborating-with-pull-requests/working-with-forks/allowing-changes-to-a-pull-request-branch-created-from-a-fork).",
            title="Pull request can't be updated with latest base branch changes",
        )


async def pre_rebase_check(ctxt: context.Context) -> None:
    pre_update_check(ctxt)

    # If PR from a private fork but cannot be edited:
    # NOTE(jd): GitHub removed the ability to configure `maintainer_can_modify` on private
    # fork we which make rebase impossible
    if (
        ctxt.pull_from_fork
        and ctxt.pull["base"]["repo"]["private"]
        and not ctxt.pull["maintainer_can_modify"]
    ):
        raise BranchUpdateFailure(
            "Mergify needs the permission to update the base branch of the pull request.\n"
            "GitHub does not allow a GitHub App to modify base branch for a private fork.\n"
            "You cannot `rebase` a pull request from a private fork.",
            title="Pull request can't be updated with latest base branch changes",
        )

    if not ctxt.can_change_github_workflow() and await ctxt.github_workflow_changed():
        raise BranchUpdateFailure(
            f"{constants.NEW_MERGIFY_PERMISSIONS_MUST_BE_ACCEPTED}"
            "In the meantime, this pull request must be rebased manually.",
            title="Pull request can't be updated with latest base branch changes",
        )


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=0.2),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception_type(BranchUpdateNeedRetry),
    reraise=True,
)
async def _do_rebase(
    ctxt: context.Context,
    user: github_user.GitHubUser,
    committer: github_user.GitHubUser | None,
    autosquash: bool,
) -> None:
    # NOTE(sileht):
    # $ curl https://api.github.com/repos/sileht/repotest/pulls/2 | jq .commits
    # 2
    # $ git clone https://XXXXX@github.com/sileht-tester/repotest \
    #           --depth=$((2 + 1)) -b sileht/testpr
    # $ cd repotest
    # $ git remote add upstream https://XXXXX@github.com/sileht/repotest.git
    # $ git log | grep Date | tail -1
    # Date:   Fri Mar 30 21:30:26 2018 (10 days ago)
    # $ git fetch upstream master --shallow-since="Fri Mar 30 21:30:26 2018"
    # $ git rebase upstream/master
    # $ git push origin sileht/testpr:sileht/testpr

    if ctxt.pull["head"]["repo"] is None:
        raise BranchUpdateFailure("The head repository does not exist anymore")

    head_branch = ctxt.pull["head"]["ref"]
    base_branch = ctxt.pull["base"]["ref"]
    async with gitter.Gitter(ctxt.log) as git:
        try:
            await git.configure(ctxt.repository.installation.redis.cache, committer)

            await git.setup_remote(
                "origin",
                ctxt.pull["head"]["repo"],
                user.oauth_access_token,
                "",
            )
            await git.setup_remote(
                "upstream",
                ctxt.pull["base"]["repo"],
                user.oauth_access_token,
                "",
            )

            await git.fetch("origin", head_branch)
            await git("checkout", "-q", "-b", head_branch, f"origin/{head_branch}")

            await git("fetch", "--quiet", "upstream", base_branch)

            if autosquash:
                await git(
                    "rebase",
                    "--interactive",
                    "--autosquash",
                    f"upstream/{base_branch}",
                    _env={"GIT_SEQUENCE_EDITOR": ":", "EDITOR": ":"},
                )
            else:
                await git("rebase", f"upstream/{base_branch}")

            await git("push", "--verbose", "origin", head_branch, "--force-with-lease")

            expected_sha = (await git("rev-parse", head_branch)).strip()
            if expected_sha:
                level = logging.INFO
            else:
                level = logging.ERROR
            ctxt.log.log(level, "pull request rebased", new_head_sha=expected_sha)
            # NOTE(sileht): We store this for Context.has_been_synchronized_by_user()
            await ctxt.redis.cache.setex(
                f"branch-update-{expected_sha}",
                60 * 60,
                expected_sha,
            )
        except gitter.GitMergifyNamespaceConflict as e:
            raise BranchUpdateFailure(
                "`Mergify uses `mergify/...` namespace for creating temporary branches. "
                "A branch of your repository is conflicting with this namespace\n"
                f"```\n{e.output}\n```\n",
            )
        except gitter.GitAuthenticationFailure:
            raise
        except gitter.GitErrorRetriable as e:
            raise BranchUpdateNeedRetry(
                f"Git reported the following error:\n```\n{e.output}\n```\n",
            )
        except gitter.GitFatalError as e:
            raise BranchUpdateFailure(
                f"Git reported the following error:\n```\n{e.output}\n```\n",
            )
        except gitter.GitError as e:
            for message, out_exception in GIT_MESSAGE_TO_EXCEPTION.items():
                if message in e.output:
                    raise out_exception(
                        f"Git reported the following error:\n```\n{e.output}\n```\n",
                    )

            ctxt.log.error(
                "update branch failed",
                output=e.output,
                returncode=e.returncode,
                exc_info=True,
            )
            raise BranchUpdateFailure("Git reported an unexpected error while rebasing")
        except Exception:  # pragma: no cover
            ctxt.log.error("update branch failed", exc_info=True)
            raise BranchUpdateFailure("Git reported an unknown error while rebasing")


async def update_with_api(
    ctxt: context.Context,
    on_behalf: github_user.GitHubUser | None = None,
) -> None:
    ctxt.log.info("updating base branch with api")
    pre_update_check(ctxt)

    if on_behalf is None:
        oauth_token = None
    else:
        oauth_token = on_behalf.oauth_access_token

    try:
        await ctxt.client.put(
            f"{ctxt.base_url}/pulls/{ctxt.pull['number']}/update-branch",
            api_version="lydian",
            oauth_token=oauth_token,
            json={"expected_head_sha": ctxt.pull["head"]["sha"]},
        )
    except http.HTTPClientSideError as e:
        if e.status_code == 422:
            # Do not use db here, because the pull request we have in db
            # might not be updated yet.
            refreshed_pull = await pull_request_getter.get_pull_request(
                ctxt.client,
                ctxt.pull["number"],
                repo_owner=ctxt.repo_owner_login,
                repo_name=ctxt.repo_name,
                force_new=True,
            )
            if refreshed_pull["head"]["sha"] != ctxt.pull["head"]["sha"]:
                ctxt.log.info(
                    "branch updated in the meantime",
                    status_code=e.status_code,
                    error=e.message,
                )
                return

        elif e.status_code == 403:
            ctxt.log.debug(
                "permission error",
                status_code=e.status_code,
                error=e.message,
                expected_head_sha=ctxt.pull["head"]["sha"],
                response_body=e.response.json(),
            )
            raise BranchUpdateFailure(
                title="Mergify doesn't have permission to update",
                message="For security reasons, Mergify can't update this pull request. "
                "Try updating locally.\n"
                f"GitHub response: {e.message}",
            )

        ctxt.log.info(
            "update branch failed",
            status_code=e.status_code,
            error=e.message,
        )
        raise BranchUpdateFailure(e.message)


async def rebase_with_git(
    ctxt: context.Context,
    on_behalf: github_user.GitHubUser,
    autosquash: bool = False,
) -> None:
    ctxt.log.info("updating base branch with git")

    await pre_rebase_check(ctxt)

    try:
        await _do_rebase(ctxt, on_behalf, on_behalf, autosquash)
    except gitter.GitAuthenticationFailure:
        ctxt.log.info(
            "git authentification failure",
            login=on_behalf.login,
            exc_info=True,
        )

        message = f"`{on_behalf.login}` token is invalid, make sure `{on_behalf.login}` can still log in on the [Mergify dashboard]({settings.DASHBOARD_UI_FRONT_URL})."

        if ctxt.pull_from_fork:
            if ctxt.pull["base"]["repo"]["private"]:
                message = "Rebasing a branch for a forked private repository is not supported by GitHub."
            else:
                permission = await ctxt.repository.get_user_permission(
                    on_behalf.to_github_account(),
                )
                if permission < github_types.GitHubRepositoryPermission.WRITE:
                    message = f"`{on_behalf.login}` does not have write access to the forked repository."

        raise BranchUpdateFailure(message)
