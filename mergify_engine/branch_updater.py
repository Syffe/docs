import dataclasses
import logging
import uuid

import tenacity

from mergify_engine import config
from mergify_engine import context
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import gitter
from mergify_engine.clients import http
from mergify_engine.dashboard import user_tokens


class BranchUpdateFailure(Exception):
    def __init__(
        self,
        msg: str = "",
        title: str = "Base branch update has failed",
    ) -> None:
        error_code = "err-code: " + uuid.uuid4().hex[-5:].upper()
        self.title = title
        self.message = msg + "\n" + error_code
        super().__init__(self.message)


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
    elif not ctxt.can_change_github_workflow() and await ctxt.github_workflow_changed():
        raise BranchUpdateFailure(
            "The new Mergify permissions must be accepted to rebase pull request with `.github/workflows` changes.\n"
            "You can accept them at https://dashboard.mergify.com/.\n"
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
    user: user_tokens.UserTokensUser,
    committer: user_tokens.UserTokensUser | None,
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
    git = gitter.Gitter(ctxt.log)
    try:
        await git.init()
        await git.configure(ctxt.repository.installation.redis.cache, committer)
        await git.setup_remote(
            "origin", ctxt.pull["head"]["repo"], user["oauth_access_token"], ""
        )
        await git.setup_remote(
            "upstream", ctxt.pull["base"]["repo"], user["oauth_access_token"], ""
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
            f"branch-update-{expected_sha}", 60 * 60, expected_sha
        )
    except gitter.GitMergifyNamespaceConflict as e:
        raise BranchUpdateFailure(
            "`Mergify uses `mergify/...` namespace for creating temporary branches. "
            "A branch of your repository is conflicting with this namespace\n"
            f"```\n{e.output}\n```\n"
        )
    except gitter.GitAuthenticationFailure:
        raise
    except gitter.GitErrorRetriable as e:
        raise BranchUpdateNeedRetry(
            f"Git reported the following error:\n```\n{e.output}\n```\n"
        )
    except gitter.GitFatalError as e:
        raise BranchUpdateFailure(
            f"Git reported the following error:\n```\n{e.output}\n```\n"
        )
    except gitter.GitError as e:
        for message, out_exception in GIT_MESSAGE_TO_EXCEPTION.items():
            if message in e.output:
                raise out_exception(
                    f"Git reported the following error:\n```\n{e.output}\n```\n"
                )

        ctxt.log.error(
            "update branch failed",
            output=e.output,
            returncode=e.returncode,
            exc_info=True,
        )
        raise BranchUpdateFailure()
    except Exception:  # pragma: no cover
        ctxt.log.error("update branch failed", exc_info=True)
        raise BranchUpdateFailure()
    finally:
        await git.cleanup()


async def update_with_api(
    ctxt: context.Context,
    on_behalf: user_tokens.UserTokensUser | None = None,
) -> None:
    ctxt.log.info("updating base branch with api")
    pre_update_check(ctxt)
    try:
        await ctxt.client.put(
            f"{ctxt.base_url}/pulls/{ctxt.pull['number']}/update-branch",
            api_version="lydian",
            oauth_token=on_behalf["oauth_access_token"] if on_behalf else None,
            json={"expected_head_sha": ctxt.pull["head"]["sha"]},
        )
    except http.HTTPClientSideError as e:
        if e.status_code == 422:
            refreshed_pull = await ctxt.client.item(
                f"{ctxt.base_url}/pulls/{ctxt.pull['number']}"
            )
            if refreshed_pull["head"]["sha"] != ctxt.pull["head"]["sha"]:
                ctxt.log.info(
                    "branch updated in the meantime",
                    status_code=e.status_code,
                    error=e.message,
                )
                return
        ctxt.log.info(
            "update branch failed",
            status_code=e.status_code,
            error=e.message,
        )
        raise BranchUpdateFailure(e.message)


async def rebase_with_git_for_github_app(
    ctxt: context.Context,
    autosquash: bool = False,
) -> None:
    ctxt.log.info("updating base branch with git")

    await pre_rebase_check(ctxt)

    # TODO(sileht):
    # * OnPremise: fail and ask customer to set update_bot_account
    # * SaaS: user synack associated-users API instead of this random user pick
    tokens = await ctxt.repository.installation.get_user_tokens()

    for user in tokens.users:
        try:
            await _do_rebase(ctxt, user, None, autosquash)
        except gitter.GitAuthenticationFailure as e:  # pragma: no cover
            ctxt.log.info(
                "authentification failure, will retry another token: %s",
                e,
                login=user["login"],
            )
        else:
            ctxt.log.warning("GitHub App pull request rebased by %s", user["login"])
            return

    ctxt.log.warning("unable to update branch: no tokens are valid")

    if ctxt.pull_from_fork and ctxt.pull["base"]["repo"]["private"]:
        message = "Rebasing a branch for a forked private repository is not supported by GitHub."
    else:
        message = "No account found to rebase a branch of GitHub App, did you set `update_bot_account`?"

    raise BranchUpdateFailure(message)


async def rebase_with_git(
    ctxt: context.Context,
    on_behalf: github_types.GitHubLogin,
    autosquash: bool = False,
) -> None:
    ctxt.log.info("updating base branch with git")

    await pre_rebase_check(ctxt)

    tokens = await ctxt.repository.installation.get_user_tokens()
    on_behalf_auth_info = tokens.get_token_for(on_behalf)
    if not on_behalf_auth_info:
        raise BranchUpdateFailure(
            f"User `{on_behalf}` is unknown, make sure `{on_behalf}` has logged in Mergify [Mergify dashboard]({config.DATABASE_URL})."
        )

    try:
        await _do_rebase(ctxt, on_behalf_auth_info, on_behalf_auth_info, autosquash)
    except gitter.GitAuthenticationFailure:
        ctxt.log.info("git authentification failure", login=on_behalf, exc_info=True)

        if ctxt.pull_from_fork and ctxt.pull["base"]["repo"]["private"]:
            message = "Rebasing a branch for a forked private repository is not supported by GitHub."
        else:
            message = f"`{on_behalf}` token is invalid, make sure `{on_behalf}` can still log in on the [Mergify dashboard]({config.DATABASE_URL})."

        raise BranchUpdateFailure(message)
