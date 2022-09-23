from unittest import mock

from datadog import statsd  # type: ignore[attr-defined]


statsd.socket = mock.Mock()
