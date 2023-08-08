import subprocess


def test_cli() -> None:
    # FIXME(sileht): this semgrep looks buggy, it think the tuple a dynamic, while it's not.
    # nosemgrep: python.lang.security.audit.dangerous-subprocess-use.dangerous-subprocess-use
    p = subprocess.run(
        ("mergify-admin", "--help"),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    assert p.returncode == 0
    assert b"Usage: mergify-admin [OPTIONS] COMMAND [ARGS]..." in p.stdout
