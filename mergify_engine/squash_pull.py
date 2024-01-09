import dataclasses

from mergify_engine import context
from mergify_engine import exceptions
from mergify_engine import gitter
from mergify_engine import settings
from mergify_engine.models.github import user as github_user


@dataclasses.dataclass
class SquashFailureError(Exception):
    reason: str


@dataclasses.dataclass
class SquashNeedRetry(exceptions.EngineNeedRetryError):
    pass


GIT_MESSAGE_TO_EXCEPTION = {
    "CONFLICT (": SquashFailureError,
}


async def _do_squash(
    ctxt: context.Context,
    user: github_user.GitHubUser,
    squash_message: str,
) -> None:
    if ctxt.pull["head"]["repo"] is None:
        raise SquashFailureError(
            f"The head repository of {ctxt.pull['base']['label']} has been deleted.",
        )

    head_branch = ctxt.pull["head"]["ref"]
    base_branch = ctxt.pull["base"]["ref"]
    tmp_branch = "squashed-head-branch"

    async with gitter.Gitter(ctxt.log) as git:
        try:
            await git.configure(ctxt.repository.installation.redis.cache, user)

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
            await git.fetch("upstream", base_branch)
            await git("checkout", "-q", "-b", tmp_branch, f"upstream/{base_branch}")

            await git("merge", "--squash", "--no-edit", f"origin/{head_branch}")
            await git("commit", "-m", squash_message)

            await git(
                "push",
                "--verbose",
                "origin",
                f"{tmp_branch}:{head_branch}",
                "--force-with-lease",
            )

            expected_sha = (await git("log", "-1", "--format=%H")).strip()
            # NOTE(sileht): We store this for dismissal action
            # FIXME(sileht): use a more generic name for the key
            await ctxt.redis.cache.setex(
                f"branch-update-{expected_sha}",
                60 * 60,
                expected_sha,
            )
        except gitter.GitMergifyNamespaceConflictError as e:
            raise SquashFailureError(
                "`Mergify uses `mergify/...` namespace for creating temporary branches. "
                "A branch of your repository is conflicting with this namespace\n"
                f"```\n{e.output}\n```\n",
            )
        except gitter.GitAuthenticationFailureError:
            raise
        except gitter.GitErrorRetriableError as e:
            raise SquashNeedRetry(
                f"Git reported the following error:\n```\n{e.output}\n```\n",
            )
        except gitter.GitFatalError as e:
            raise SquashFailureError(
                f"Git reported the following error:\n```\n{e.output}\n```\n",
            )
        except gitter.GitError as e:
            for message, out_exception in GIT_MESSAGE_TO_EXCEPTION.items():
                if message in e.output:
                    raise out_exception(
                        f"Git reported the following error:\n```\n{e.output}\n```\n",
                    )

            ctxt.log.error(
                "squash failed",
                output=e.output,
                returncode=e.returncode,
                exc_info=True,
            )
            raise SquashFailureError("")
        except Exception:  # pragma: no cover
            ctxt.log.error("squash failed", exc_info=True)
            raise SquashFailureError("")


async def squash(
    ctxt: context.Context,
    message: str,
    on_behalf: github_user.GitHubUser,
) -> None:
    if ctxt.pull["commits"] <= 1:
        return

    try:
        await _do_squash(ctxt, on_behalf, message)
    except gitter.GitAuthenticationFailureError:
        ctxt.log.info("git authentification failure", login=on_behalf, exc_info=True)

        if ctxt.pull_from_fork and ctxt.pull["base"]["repo"]["private"]:
            message = "Squashing a branch for a forked private repository is not supported by GitHub."
        else:
            message = f"`{on_behalf}` token is invalid, make sure `{on_behalf}` can still log in on the [Mergify dashboard]({settings.DASHBOARD_UI_FRONT_URL})."

        raise SquashFailureError(message)
