import dataclasses

from mergify_engine import context
from mergify_engine import exceptions
from mergify_engine import gitter
from mergify_engine.dashboard import user_tokens
from mergify_engine.dashboard.subscription import Features


@dataclasses.dataclass
class SquashFailure(Exception):
    reason: str


@dataclasses.dataclass
class SquashNeedRetry(exceptions.EngineNeedRetry):
    message: str


GIT_MESSAGE_TO_EXCEPTION = {
    "CONFLICT (content): ": SquashFailure,
}


async def _do_squash(
    ctxt: context.Context, user: user_tokens.UserTokensUser, squash_message: str
) -> None:

    head_branch = ctxt.pull["head"]["ref"]
    base_branch = ctxt.pull["base"]["ref"]
    tmp_branch = "squashed-head-branch"

    git = gitter.Gitter(ctxt.log)

    if ctxt.pull["head"]["repo"] is None:
        raise SquashFailure(
            f"The head repository of {ctxt.pull['base']['label']} has been deleted."
        )

    try:
        await git.init()

        if ctxt.subscription.has_feature(Features.BOT_ACCOUNT):
            await git.configure(user)
        else:
            await git.configure()

        await git.setup_remote(
            "origin", ctxt.pull["head"]["repo"], user["oauth_access_token"], ""
        )
        await git.setup_remote(
            "upstream", ctxt.pull["base"]["repo"], user["oauth_access_token"], ""
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
            f"branch-update-{expected_sha}", 60 * 60, expected_sha
        )
    except gitter.GitMergifyNamespaceConflict as e:
        raise SquashFailure(
            "`Mergify uses `mergify/...` namespace for creating temporary branches. "
            "A branch of your repository is conflicting with this namespace\n"
            f"```\n{e.output}\n```\n"
        )
    except gitter.GitAuthenticationFailure:
        raise
    except gitter.GitErrorRetriable as e:
        raise SquashNeedRetry(
            f"Git reported the following error:\n```\n{e.output}\n```\n"
        )
    except gitter.GitFatalError as e:
        raise SquashFailure(
            f"Git reported the following error:\n```\n{e.output}\n```\n"
        )
    except gitter.GitError as e:
        for message, out_exception in GIT_MESSAGE_TO_EXCEPTION.items():
            if message in e.output:
                raise out_exception(
                    f"Git reported the following error:\n```\n{e.output}\n```\n"
                )

        ctxt.log.error(
            "squash failed",
            output=e.output,
            returncode=e.returncode,
            exc_info=True,
        )
        raise SquashFailure("")
    except Exception:  # pragma: no cover
        ctxt.log.error("squash failed", exc_info=True)
        raise SquashFailure("")
    finally:
        await git.cleanup()


async def squash(
    ctxt: context.Context, message: str, users: list[user_tokens.UserTokensUser]
) -> None:

    if ctxt.pull["commits"] <= 1:
        return

    for user in users:
        try:
            await _do_squash(ctxt, user, message)
        except gitter.GitAuthenticationFailure as e:
            ctxt.log.info(
                "authentification failure, will retry another token: %s",
                e,
                login=user["login"],
            )
        else:
            return

    ctxt.log.warning("unable to squash pull request: no tokens are valid")

    if ctxt.pull_from_fork and ctxt.pull["base"]["repo"]["private"]:
        raise SquashFailure(
            "Squashing a branch for a forked private repository is not supported by GitHub"
        )

    raise SquashFailure("No oauth valid tokens")
