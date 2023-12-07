import os

import httpx
import pytest

from mergify_engine import settings
from mergify_engine.tests import conftest


MODULE_DIRECTORY_PATH = os.path.dirname(__file__)
FAKE_REACT_BUILD_DIR = os.path.abspath(f"{MODULE_DIRECTORY_PATH}/fake-build")


@pytest.fixture()
def _set_dashboard_ui_static_files_directory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # NOTE(sileht): must be done before `create_app` is called
    monkeypatch.setattr(
        settings,
        "DASHBOARD_UI_STATIC_FILES_DIRECTORY",
        FAKE_REACT_BUILD_DIR,
    )


async def test_react_static_files(
    _set_dashboard_ui_static_files_directory: None,
    web_client_with_fresh_web_app: conftest.CustomTestClient,
) -> None:
    # NOTE(sileht): httpx remove the .., we want to keep them for the test
    prepared_request = web_client_with_fresh_web_app.build_request(
        # nosemgrep: python.lang.security.audit.insecure-transport.requests.request-with-http.request-with-http
        method="GET",
        url="http://localhost:5001/assets/../../test_react.py",
    )
    # nosemgrep: python.lang.security.audit.insecure-transport.requests.request-with-http.request-with-http
    prepared_request.url = httpx.URL("http://localhost:5001/assets/../../test_react.py")
    response = await web_client_with_fresh_web_app.send(prepared_request)
    assert response.status_code == 404

    response = await web_client_with_fresh_web_app.get("/assets/")
    assert response.status_code == 404

    response = await web_client_with_fresh_web_app.get("/assets")
    assert response.status_code == 404

    response = await web_client_with_fresh_web_app.head("/assets/notexits.js")
    assert response.status_code == 404

    response = await web_client_with_fresh_web_app.get("/assets/notexits.js")
    assert response.status_code == 404

    response = await web_client_with_fresh_web_app.get("/index.html")
    assert response.status_code == 200
    assert response.content == b"testing\n"

    response = await web_client_with_fresh_web_app.get("/assets/main.js")
    assert response.status_code == 200
    assert response.content == b"testing\n"

    response = await web_client_with_fresh_web_app.head("/assets/main.js")
    assert response.status_code == 200
    assert response.content == b""
