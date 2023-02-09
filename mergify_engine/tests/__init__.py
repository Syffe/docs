from unittest import mock

from datadog import statsd  # type: ignore[attr-defined]

from mergify_engine import worker_pusher


statsd.socket = mock.Mock()  # type: ignore[assignment]


worker_pusher.WORKER_PROCESSING_DELAY = 0.01
