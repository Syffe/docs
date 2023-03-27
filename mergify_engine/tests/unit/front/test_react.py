import os

import httpx
import pytest

from mergify_engine import settings
from mergify_engine.tests import conftest


MODULE_DIRECTORY_PATH = os.path.dirname(__file__)
FAKE_REACT_BUILD_DIR = os.path.abspath(f"{MODULE_DIRECTORY_PATH}/fake-build")


@pytest.fixture
def set_dashboard_ui_static_files_directory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # NOTE(sileht): must be done before web_client is created
    monkeypatch.setattr(
        settings, "DASHBOARD_UI_STATIC_FILES_DIRECTORY", FAKE_REACT_BUILD_DIR
    )


async def test_react_static_files(
    set_dashboard_ui_static_files_directory: None,
    web_client: conftest.CustomTestClient,
) -> None:
    # NOTE(sileht): httpx remove the .., we want to keep them for the test
    prepared_request = web_client.build_request(
        # nosemgrep: python.lang.security.audit.insecure-transport.requests.request-with-http.request-with-http
        method="GET",
        url="http://localhost:5001/static/../../test_react.py",
    )
    # nosemgrep: python.lang.security.audit.insecure-transport.requests.request-with-http.request-with-http
    prepared_request.url = httpx.URL("http://localhost:5001/static/../../test_react.py")
    response = await web_client.send(prepared_request)
    assert response.status_code == 404

    response = await web_client.get("/static/")
    assert response.status_code == 404

    response = await web_client.get("/static")
    assert response.status_code == 404

    response = await web_client.head("/static/notexits.js")
    assert response.status_code == 404

    response = await web_client.get("/static/notexits.js")
    assert response.status_code == 404

    response = await web_client.get("/manifest.json")
    assert response.status_code == 200
    assert response.content == b"testing\n"

    response = await web_client.get("/static/main.js")
    assert response.status_code == 200
    assert response.content == b"testing\n"

    response = await web_client.head("/static/main.js")
    assert response.status_code == 200
    assert response.content == b""
