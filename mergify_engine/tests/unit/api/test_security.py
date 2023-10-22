import httpx
import pytest


@pytest.mark.parametrize(
    "owner",
    (
        "toolonggggggggggggggggggggggggggggggggggggg",
        "-noway",
        "noway-",
        "-noway-",
        "maybe%00not",
        "nop%C3%A9nop",
        "nop!nop",
        "nop:nop",
        "nop_nop",
    ),
)
async def test_bad_owner(web_client: httpx.AsyncClient, owner: str) -> None:
    reply = await web_client.get(f"/v1/badges/{owner}/repo.png", follow_redirects=False)
    assert reply.status_code == 422


@pytest.mark.parametrize(
    "repo",
    (
        ("not!!really"),
        # ENGINE-3K3
        ("soongood", "StudentRevision%7Cecho%20ef7tjagc8m%207cegzqi9m4%7C%7Ca%20"),
    ),
)
async def test_bad_repo(web_client: httpx.AsyncClient, repo: str) -> None:
    reply = await web_client.get(f"/v1/badges/owner/{repo}.png", follow_redirects=False)
    assert reply.status_code == 422


@pytest.mark.parametrize(
    "full_repo",
    (
        "its-ok/.ok-",
        "right123name/-ok",
        "right-123-name/-ok",
        "right-123-name/_ok",
        "right-123-name/_ok_",
        "right123-name/ok.",
        "right-123-name/-ok_123.",
        "right123name/-ok_",
    ),
)
async def test_valid_owner_repo(web_client: httpx.AsyncClient, full_repo: str) -> None:
    reply = await web_client.get(f"/v1/badges/{full_repo}.png", follow_redirects=False)
    assert reply.status_code == 302
