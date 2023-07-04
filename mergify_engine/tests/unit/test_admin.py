import subprocess
import sys
from unittest import mock

from mergify_engine import admin


def test_admin_tool_installation() -> None:
    # FIXME(sileht): this semgrep looks buggy, it think the tuple a dynamic, while it's not.
    # nosemgrep: python.lang.security.audit.dangerous-subprocess-use.dangerous-subprocess-use
    p = subprocess.run(
        ("mergify-admin", "--help"),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    assert p.returncode == 0
    assert b"usage: mergify-admin [-h]" in p.stdout


@mock.patch.object(sys, "argv", ["mergify-admin", "suspend", "foobar"])
@mock.patch.object(admin, "suspended")
def test_admin_suspend(suspended: mock.Mock) -> None:
    admin.main()
    assert suspended.mock_calls == [mock.call("PUT", "foobar")]


@mock.patch.object(sys, "argv", ["mergify-admin", "unsuspend", "foobar"])
@mock.patch.object(admin, "suspended")
def test_admin_unsuspend(suspended: mock.Mock) -> None:
    admin.main()
    assert suspended.mock_calls == [mock.call("DELETE", "foobar")]
