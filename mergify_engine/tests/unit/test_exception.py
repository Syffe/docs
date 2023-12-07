import httpx
import pytest

from mergify_engine import exceptions
from mergify_engine.clients import http


def test_exception_ip_forbidden() -> None:
    response = httpx.Response(
        status_code=403,
        json={
            "message": "Although you appear to have the correct authorization credentials, the `org-login` organization has an IP allow list enabled, and 44.197.229.19 is not permitted to access this resource.",
            "documentation_url": "https://docs.github.com/rest/reference/pulls#list-pull-requests",
        },
        request=httpx.Request(method="GET", url="http://example.Com"),
    )
    with pytest.raises(http.HTTPForbidden) as exc:
        http.raise_for_status(response)

    assert exceptions.should_be_ignored(exc.value)
