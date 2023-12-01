import asyncio
import collections
import dataclasses
import datetime
import logging
import os
import re
import sys
import types
import typing
import urllib.parse

import aiofiles.os
import aiofiles.tempfile

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.models.github import user as github_user


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


class GitTimeout(GitFatalError):
    pass


GIT_MESSAGE_TO_EXCEPTION: dict[
    str | re.Pattern[str],
    type[GitError],
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
        ("The requested URL returned error: 500", GitErrorRetriable),
        ("The requested URL returned error: 501", GitErrorRetriable),
        ("The requested URL returned error: 502", GitErrorRetriable),
        ("The requested URL returned error: 503", GitErrorRetriable),
        ("The requested URL returned error: 504", GitErrorRetriable),
        ("remote contains work that you do", GitErrorRetriable),
        ("remote end hung up unexpectedly", GitErrorRetriable),
        ("gnutls_handshake() failed:", GitErrorRetriable),
        ("unexpected disconnect while reading sideband packet", GitErrorRetriable),
        # https://github.com/orgs/community/discussions/15823
        ("fatal error in commit_refs ", GitErrorRetriable),
        (
            re.compile(
                "cannot lock ref 'refs/heads/mergify/[^']*': 'refs/heads/mergify(|/bp|/copy|/merge-queue)' exists",
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
    ],
)


@dataclasses.dataclass
class Gitter:
    logger: "logging.LoggerAdapter[logging.Logger]"
    tmp: str | None = None
    _messages: list[tuple[str, dict[str, typing.Any]]] = dataclasses.field(
        default_factory=list,
    )
    _log_level: int = logging.INFO
    _td: aiofiles.tempfile.AiofilesContextManagerTempDir[
        None,
        None,
        aiofiles.tempfile.temptypes.AsyncTemporaryDirectory,
    ] = dataclasses.field(
        default_factory=lambda: aiofiles.tempfile.TemporaryDirectory(
            prefix="mergify-gitter",
        ),
    )

    GIT_COMMAND_TIMEOUT: float = dataclasses.field(
        init=False,
        default=datetime.timedelta(minutes=10).total_seconds(),
    )

    async def __aenter__(self) -> "Gitter":
        await self.init()
        return self

    async def init(self) -> None:
        self.tmp = await self._td.__aenter__()
        if self.tmp is None:
            # This is to please mypy only as it never returns None
            raise RuntimeError("Unable to create temporary directory?")
        self.repository = os.path.join(self.tmp, "repository")
        await aiofiles.os.mkdir(self.repository)

        self.env = {
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_NOGLOB_PATHSPECS": "1",
            "GIT_PROTOCOL_FROM_USER": "0",
            "GIT_ALLOW_PROTOCOL": "https",
            "PATH": os.environ["PATH"],
            "HOME": self.tmp,
            "TMPDIR": self.tmp,
            "LANG": "C.UTF-8",
        }
        version = await self("version")
        self.log("git directory created", path=self.tmp, version=version)
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

    def log(self, message: str, level: int = logging.INFO, **extra: typing.Any) -> None:
        self._log_level = max(level, self._log_level)
        self._messages.append((message, extra))

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

        command = ("git", *args)
        self.log("git operation", command=command)

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

            try:
                async with asyncio.timeout(self.GIT_COMMAND_TIMEOUT):
                    stdout, _ = await process.communicate(
                        input=None if _input is None else _input.encode("utf8"),
                    )
            except asyncio.TimeoutError:
                self.log(
                    "git operation timed out",
                    command=command,
                    level=logging.ERROR,
                )
                raise GitTimeout(-1, "git operation took too long")

            output = stdout.decode("utf-8")
            self._check_git_output(process, output)
        finally:
            self.log("git operation finished", command=command)

        return output

    def _check_git_output(
        self,
        process: asyncio.subprocess.Process,
        output: str,
    ) -> None:
        if process.returncode:
            raise self._create_git_exception(process.returncode, output)

    @classmethod
    def _create_git_exception(cls, returncode: int, output: str) -> GitError:
        if output == "" or returncode == -15:
            # SIGKILL...
            return GitErrorRetriable(returncode, "Git process got killed")

        if cls._is_force_push_lease_reject(output):
            return GitErrorRetriable(
                returncode,
                f"Remote branch changed in the meantime: \n```\n{output}\n```\n",
            )

        for pattern, out_exception in GIT_MESSAGE_TO_EXCEPTION.items():
            if isinstance(pattern, re.Pattern):
                match = pattern.search(output) is not None
            else:
                match = pattern in output
            if match:
                return out_exception(returncode, output)

        return GitError(returncode, output)

    @staticmethod
    def _is_force_push_lease_reject(message: str) -> bool:
        return (
            "failed to push some refs" in message
            and "[rejected]" in message
            and "(stale info)" in message
        )

    async def __aexit__(
        self,
        exc_type: type[Exception] | None,
        exc_value: Exception | None,
        traceback: types.TracebackType | None,
    ) -> None:
        await self.cleanup()

    async def cleanup(self) -> None:
        self.log(f"cleaning: {self.tmp}")

        try:
            try:
                await self(
                    "credential-cache",
                    f"--socket={self.tmp}/.git-creds-socket",
                    "exit",
                )
            except GitError:  # pragma: no cover
                self.log("git credential-cache exit fail", level=logging.ERROR)

            ongoing_exc_type, ongoing_exc_value, ongoing_tb = sys.exc_info()
            try:
                await self._td.__aexit__(
                    ongoing_exc_type,
                    ongoing_exc_value,
                    ongoing_tb,
                )
            except OSError:
                self.log("git temporary directory cleanup fail.")
        finally:
            self.logger.log(self._log_level, "gitter messages", messages=self._messages)

    async def configure(
        self,
        redis_cache: redis_utils.RedisCache,
        user: github_user.GitHubUser | None = None,
    ) -> None:
        if user is None:
            name = "Mergify"
            mergify_bot = await github.GitHubAppInfo.get_bot(redis_cache)
            login = mergify_bot["login"]
            account_id = mergify_bot["id"]
        else:
            name = user.login
            login = user.login
            account_id = user.id

        await self("config", "user.name", name)
        await self(
            "config",
            "user.email",
            f"{account_id}+{login}@users.noreply.{settings.GITHUB_URL.host}",
        )

    async def add_cred(self, username: str, password: str, path: str) -> None:
        parsed = list(urllib.parse.urlparse(settings.GITHUB_URL))
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
            "remote",
            "add",
            name,
            f"{settings.GITHUB_URL}/{repository['full_name']}",
        )
        await self("config", f"remote.{name}.promisor", "true")
        await self("config", f"remote.{name}.partialclonefilter", "blob:none")
