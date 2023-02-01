import asyncio.base_subprocess
import sys
from unittest import mock


# NOTE(sileht): workaround python 3.11.1 from:
# https://github.com/python/cpython/pull/100398
def _process_exited(self, returncode):  # type: ignore
    self._returncode = returncode
    if self._proc.returncode is None:
        # asyncio uses a child watcher: copy the status into the Popen
        # object. On Python 3.6, it is required to avoid a ResourceWarning.
        self._proc.returncode = returncode
    self._call(self._protocol.process_exited)
    self._try_finish()


if not (
    sys.version_info.major >= 3
    and sys.version_info.minor >= 11
    and sys.version_info.micro > 1
):
    mock.patch.object(
        asyncio.base_subprocess.BaseSubprocessTransport,
        "_process_exited",
        _process_exited,
    ).start()
