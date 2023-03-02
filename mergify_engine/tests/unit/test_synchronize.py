from collections import abc
import typing
from unittest import mock

from mergify_engine import check_api
from mergify_engine import github_types
from mergify_engine.tests.unit import conftest


async def test_summary_synchronization_cache(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    async def items(
        *args: typing.Any, **kwargs: typing.Any
    ) -> abc.AsyncGenerator[None, None]:
        for _ in []:
            yield
        return

    async def post_check(*args: typing.Any, **kwargs: typing.Any) -> mock.Mock:
        return mock.Mock(
            status_code=200,
            json=mock.Mock(
                return_value=github_types.GitHubCheckRun(
                    {
                        "head_sha": github_types.SHAType(
                            "ce587453ced02b1526dfb4cb910479d431683101"
                        ),
                        "details_url": "https://example.com",
                        "status": "completed",
                        "conclusion": "neutral",
                        "name": "neutral",
                        "id": 1236,
                        "app": {
                            "id": 1234,
                            "name": "CI",
                            "slug": "ci",
                            "owner": {
                                "type": "User",
                                "id": github_types.GitHubAccountIdType(1234),
                                "login": github_types.GitHubLogin("goo"),
                                "avatar_url": "https://example.com",
                            },
                        },
                        "external_id": "",
                        "pull_requests": [],
                        "before": github_types.SHAType(
                            "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                        ),
                        "after": github_types.SHAType(
                            "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                        ),
                        "started_at": github_types.ISODateTimeType(""),
                        "completed_at": github_types.ISODateTimeType(""),
                        "html_url": "https://example.com",
                        "check_suite": {"id": 1234},
                        "output": {
                            "summary": "",
                            "title": "It runs!",
                            "text": "",
                            "annotations": [],
                            "annotations_count": 0,
                            "annotations_url": "https://example.com",
                        },
                    }
                )
            ),
        )

    client = mock.AsyncMock()
    client.auth.get_access_token.return_value = "<token>"
    client.items = items
    client.post.side_effect = post_check

    ctxt = await context_getter(github_types.GitHubPullRequestNumber(6))
    ctxt.repository.installation.client = client
    assert await ctxt.get_cached_last_summary_head_sha() is None
    await ctxt.set_summary_check(
        check_api.Result(check_api.Conclusion.SUCCESS, "foo", "bar")
    )

    assert await ctxt.get_cached_last_summary_head_sha() == "the-head-sha"
    await ctxt.clear_cached_last_summary_head_sha()

    assert await ctxt.get_cached_last_summary_head_sha() is None
