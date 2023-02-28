import asyncio
import collections
import dataclasses
import logging
import os
import re
import shutil
import sys
import tempfile
import urllib.parse

from mergify_engine import config
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.clients import github
from mergify_engine.dashboard import user_tokens


@dataclasses.dataclass
class GitError(Exception):
    returncode: int
    output: str


class GitFatalError(GitError):
    pass


class GitErrorRetriable(GitError):
    pass


class GitAuthenticationFailure(GitError):
    pass


class GitReferenceAlreadyExists(GitError):
    pass


class GitMergifyNamespaceConflict(GitError):
    pass


GIT_MESSAGE_TO_EXCEPTION: dict[
    str | re.Pattern[str], type[GitError]
] = collections.OrderedDict(
    [
        ("Authentication failed", GitAuthenticationFailure),
        ("RPC failed; HTTP 401", GitAuthenticationFailure),
        ("This repository was archived so it is read-only.", GitFatalError),
        ("organization has enabled or enforced SAML SSO.", GitFatalError),
        ("fatal: bad config", GitFatalError),
        ("Invalid username or password", GitAuthenticationFailure),
        ("Repository not found", GitAuthenticationFailure),
        ("The requested URL returned error: 403", GitAuthenticationFailure),
        ("The requested URL returned error: 504", GitErrorRetriable),
        ("remote contains work that you do", GitErrorRetriable),
        ("remote end hung up unexpectedly", GitErrorRetriable),
        ("gnutls_handshake() failed:", GitErrorRetriable),
        ("unexpected disconnect while reading sideband packet", GitErrorRetriable),
        (
            re.compile(
                "cannot lock ref 'refs/heads/mergify/[^']*': 'refs/heads/mergify(|/bp|/copy|/merge-queue)' exists"
            ),
            GitMergifyNamespaceConflict,
        ),
        ("cannot lock ref 'refs/heads/", GitErrorRetriable),
        ("Could not resolve host", GitErrorRetriable),
        ("Operation timed out", GitErrorRetriable),
        ("Connection timed out", GitErrorRetriable),
        ("Couldn't connect to server", GitErrorRetriable),
        ("No such device or address", GitErrorRetriable),
        ("Protected branch update failed", GitFatalError),
        ("couldn't find remote ref", GitFatalError),
    ]
)


@dataclasses.dataclass
class Gitter:
    logger: "logging.LoggerAdapter[logging.Logger]"
    tmp: str | None = None

    # Worker timeout at 5 minutes, so ensure subprocess return before
    GIT_COMMAND_TIMEOUT: int = dataclasses.field(init=False, default=4 * 60 + 30)

    async def init(self) -> None:
        # TODO(sileht): use aiofiles instead of thread
        self.tmp = await asyncio.to_thread(tempfile.mkdtemp, prefix="mergify-gitter")
        if self.tmp is None:
            raise RuntimeError("mkdtemp failed")
        self.repository = os.path.join(self.tmp, "repository")
        # TODO(sileht): use aiofiles instead of thread
        await asyncio.to_thread(os.mkdir, self.repository)

        self.env = {
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_NOGLOB_PATHSPECS": "1",
            "GIT_PROTOCOL_FROM_USER": "0",
            "GIT_ALLOW_PROTOCOL": "https",
            "PATH": os.environ["PATH"],
            "HOME": self.tmp,
            "TMPDIR": self.tmp,
        }
        version = await self("version")
        self.logger.info("git directory created", path=self.tmp, version=version)
        await self("init", "--initial-branch=tmp-mergify-trunk")
        # NOTE(sileht): Bump the repository format. This ensures required
        # extensions (promisor, partialclonefilter) are present in git cli and
        # raise an error if not. Avoiding git cli to fallback to full clone
        # behavior for us.
        await self("config", "core.repositoryformatversion", "1")
        # Disable gc since this is a thrown-away repository
        await self("config", "gc.auto", "0")
        # Use one git cache daemon per Gitter
        await self("config", "credential.useHttpPath", "true")
        await self(
            "config",
            "credential.helper",
            f"cache --timeout=300 --socket={self.tmp}/.git-creds-socket",
        )
        # Setting the number of checkout workers to 0 will use as many workers
        # as there are logical cores on the machine.
        # https://git-scm.com/docs/git-config#Documentation/git-config.txt-checkoutworkers
        await self("config", "checkout.workers", "0")

    def prepare_safe_env(
        self,
        _env: dict[str, str] | None = None,
    ) -> dict[str, str]:
        safe_env = self.env.copy()
        if _env is not None:
            safe_env.update(_env)
        return safe_env

    async def __call__(
        self,
        *args: str,
        _input: str | None = None,
        _env: dict[str, str] | None = None,
    ) -> str:
        if self.repository is None:
            raise RuntimeError("__call__() called before init()")

        self.logger.info("calling: %s", " ".join(args))

        try:
            # TODO(sileht): Current user provided data in git commands are safe, but we should create an
            # helper function for each git command to double check the input is
            # safe, eg: like a second seatbelt. See: MRGFY-930
            # nosemgrep: python.lang.security.audit.dangerous-asyncio-create-exec.dangerous-asyncio-create-exec
            process = await asyncio.create_subprocess_exec(
                "git",
                *args,
                cwd=self.repository,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                stdin=None if _input is None else asyncio.subprocess.PIPE,
                env=self.prepare_safe_env(_env),
            )

            stdout, _ = await asyncio.wait_for(
                process.communicate(
                    input=None if _input is None else _input.encode("utf8")
                ),
                self.GIT_COMMAND_TIMEOUT,
            )
            output = stdout.decode("utf-8")
            self._check_git_output(process, output)
        finally:
            self.logger.debug("finish: %s", " ".join(args))

        return output

    def _check_git_output(
        self, process: asyncio.subprocess.Process, output: str
    ) -> None:
        if process.returncode:
            raise self._create_git_exception(process.returncode, output)

    @classmethod
    def _create_git_exception(cls, returncode: int, output: str) -> GitError:
        if output == "" or returncode == -15:
            # SIGKILL...
            return GitErrorRetriable(returncode, "Git process got killed")
        elif cls._is_force_push_lease_reject(output):
            return GitErrorRetriable(
                returncode,
                "Remote branch changed in the meantime: \n" f"```\n{output}\n```\n",
            )

        for pattern, out_exception in GIT_MESSAGE_TO_EXCEPTION.items():
            if isinstance(pattern, re.Pattern):
                match = pattern.search(output) is not None
            else:
                match = pattern in output
            if match:
                return out_exception(
                    returncode,
                    output,
                )

        return GitError(returncode, output)

    @staticmethod
    def _is_force_push_lease_reject(message: str) -> bool:
        return (
            "failed to push some refs" in message
            and "[rejected]" in message
            and "(stale info)" in message
        )

    async def cleanup(self) -> None:
        if self.tmp is None:
            return

        self.logger.info("cleaning: %s", self.tmp)
        try:
            await self(
                "credential-cache",
                f"--socket={self.tmp}/.git-creds-socket",
                "exit",
            )
        except GitError:  # pragma: no cover
            self.logger.warning("git credential-cache exit fail")
        # TODO(sileht): use aiofiles instead of thread

        ongoing_exc_type, ongoing_exc_value, _ = sys.exc_info()
        try:
            await asyncio.to_thread(shutil.rmtree, self.tmp)
        except OSError:
            if (
                ongoing_exc_type is not None
                and ongoing_exc_value is not None
                and ongoing_exc_type is asyncio.CancelledError
                # NOTE(sileht): The reason is set by worker.py
                and ongoing_exc_value.args[0] == "shutdown"
            ):
                return

            self.logger.warning("git temporary directory cleanup fail.")

    async def configure(
        self,
        redis_cache: redis_utils.RedisCacheBytes,
        user: user_tokens.UserTokensUser | None = None,
    ) -> None:
        if user is None:
            name = "Mergify"
            mergify_bot = await github.GitHubAppInfo.get_bot(redis_cache)
            login = mergify_bot["login"]
            account_id = mergify_bot["id"]
        else:
            name = user["name"] or user["login"]
            login = user["login"]
            account_id = user["id"]

        await self("config", "user.name", name)
        await self(
            "config",
            "user.email",
            f"{account_id}+{login}@users.noreply.{config.GITHUB_DOMAIN}",
        )

    async def add_cred(self, username: str, password: str, path: str) -> None:
        parsed = list(urllib.parse.urlparse(config.GITHUB_URL))
        parsed[1] = f"{username}:{password}@{parsed[1]}"
        parsed[2] = path
        url = urllib.parse.urlunparse(parsed)
        await self("credential", "approve", _input=f"url={url}\n\n")

    async def fetch(
        self,
        remote: str,
        branch: str,
    ) -> None:
        await self("fetch", "--quiet", "--no-tags", remote, branch)

    async def setup_remote(
        self,
        name: str,
        repository: github_types.GitHubRepository,
        username: str,
        password: str,
    ) -> None:
        await self.add_cred(username, password, repository["full_name"])
        await self(
            "remote", "add", name, f"{config.GITHUB_URL}/{repository['full_name']}"
        )
        await self("config", f"remote.{name}.promisor", "true")
        await self("config", f"remote.{name}.partialclonefilter", "blob:none")
