from starlette import testclient

from mergify_engine.web import root


def test_api_badge() -> None:
    with testclient.TestClient(root.app) as client:
        reply = client.get(
            "/v1/badges/mergifyio/mergify-engine.png", allow_redirects=False
        )
        assert reply.status_code == 302
        assert reply.headers["Location"] == (
            "https://img.shields.io/endpoint.png"
            "?url=https://dashboard.mergify.com/badges/mergifyio/mergify-engine&style=flat"
        )

        reply = client.get(
            "/v1/badges/mergifyio/mergify-engine.svg", allow_redirects=False
        )
        assert reply.status_code == 302
        assert reply.headers["Location"] == (
            "https://img.shields.io/endpoint.svg"
            "?url=https://dashboard.mergify.com/badges/mergifyio/mergify-engine&style=flat"
        )

        reply = client.get("/v1/badges/mergifyio/mergify-engine", allow_redirects=False)
        assert reply.headers["Location"] == (
            "https://dashboard.mergify.com/badges/mergifyio/mergify-engine"
        )
