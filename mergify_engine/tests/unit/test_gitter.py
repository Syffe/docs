import os
from unittest import mock

import pytest

from mergify_engine import gitter
from mergify_engine import redis_utils


async def test_gitter(
    monkeypatch: pytest.MonkeyPatch,
    redis_links: redis_utils.RedisLinks,
) -> None:
    monkeypatch.setenv("LANG", "C")
    git = gitter.Gitter(mock.Mock())
    try:
        await git.init()
        await git.configure(redis_links.cache_bytes)
        await git.add_cred("foo", "bar", "https://github.com")

        with pytest.raises(gitter.GitError) as exc_info:
            await git("add", "toto")
        assert exc_info.value.returncode == 128
        assert (
            exc_info.value.output == "fatal: pathspec 'toto' did not match any files\n"
        )

        if git.repository is None:
            pytest.fail("No tmp dir")

        with open(git.repository + "/.mergify.yml", "w") as f:
            f.write("pull_request_rules:")
        await git("add", ".mergify.yml")
        await git("commit", "-m", "Intial commit", "-a", "--no-edit")

        assert os.path.exists(f"{git.repository}/.git")
    finally:
        await git.cleanup()
        assert not os.path.exists(f"{git.repository}/.git")


@pytest.mark.parametrize(
    "stdout, exception, exc_message",
    (
        (
            """To https://github.com/owner/repo
 ! [remote rejected] mergify/bp/main/pr-42 -> mergify/bp/main/pr-42 (cannot lock ref 'refs/heads/mergify/bp/main/pr-42': 'refs/heads/mergify/merge-queue' exists; cannot create 'refs/heads/mergify/bp/main/pr-42')
error: failed to push some refs to 'https://github.com/owner/repo'

""",
            gitter.GitMergifyNamespaceConflict,
            "cannot lock ref",
        ),
        (
            """To https://github.com/owner/repo
 ! [remote rejected] mergify/bp/main/pr-42 -> mergify/bp/main/pr-42 (cannot lock ref 'refs/heads/mergify/bp/main/pr-42': 'refs/heads/mergify' exists; cannot create 'refs/heads/mergify/bp/main/pr-42')
error: failed to push some refs to 'https://github.com/owner/repo'

""",
            gitter.GitMergifyNamespaceConflict,
            "cannot lock ref",
        ),
        (
            """
To gitlab.com:example/my-project.git
 ! [rejected]        my-branch -> my-branch (stale info)
error: failed to push some refs to 'git@gitlab.com:example/my-project.git'
""",
            gitter.GitErrorRetriable,
            "Remote branch changed in the meantime",
        ),
    ),
)
def test_gitter_error_catching(
    stdout: str, exception: type[Exception], exc_message: str
) -> None:
    with pytest.raises(exception) as excinfo:
        raise gitter.Gitter._create_git_exception(1, stdout)
    assert exc_message in str(excinfo.value)
