from unittest import mock

import pytest
import respx

from mergify_engine import condition_value_querier
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import rules
from mergify_engine.actions import merge
from mergify_engine.actions import merge_base
from mergify_engine.clients import http
from mergify_engine.tests.unit import conftest


@pytest.mark.parametrize(
    ("body", "title", "message", "template"),
    [
        (
            """Hello world

# Commit Message
my title

my body""",
            "my title",
            "my body",
            None,
        ),
        (
            """Hello world

# Commit Message:
my title

my body
is longer""",
            "my title",
            "my body\nis longer",
            None,
        ),
        (
            """Hello world

# Commit Message
{{title}}

Authored-By: {{author}}
on two lines""",
            "My PR title",
            "Authored-By: contributor\non two lines",
            None,
        ),
        (
            """Hello world
again!

## Commit Message
My Title

CI worked:
{% for ci in status_success %}
- {{ci}}
{% endfor %}
""",
            "My Title",
            "CI worked:\n\n- my CI\n",
            None,
        ),
        (
            """Hello world

# Commit Message

my title

my body""",
            "my title",
            "my body",
            None,
        ),
        (
            """Hello world

# Commit Message

my title     
WATCHOUT ^^^ there is empty spaces above for testing ^^^^
my body""",  # noqa: W291
            "my title",
            "WATCHOUT ^^^ there is empty spaces above for testing ^^^^\nmy body",
            None,
        ),
        (
            # Should return an empty message
            """Hello world

# Commit Message

my title
""",
            "my title",
            "",
            None,
        ),
        (
            "",
            "My PR title (#43)",
            "",
            "{{title}} (#{{number}})\n\n{{body}}",
        ),
    ],
)
async def test_merge_commit_message(
    body: str,
    title: str,
    message: str,
    template: str | None,
    context_getter: conftest.ContextGetterFixture,
) -> None:
    ctxt = await context_getter(
        github_types.GitHubPullRequestNumber(43),
        body=body,
        title="My PR title",
    )
    ctxt.repository._caches.branch_protections[
        github_types.GitHubRefType("main")
    ] = None
    ctxt.repository._caches.mergify_config.set(rules.UserConfigurationSchema({}))
    ctxt._caches.pull_statuses.set(
        [
            github_types.GitHubStatus(
                {
                    "id": 1,
                    "target_url": "http://example.com",
                    "context": "my CI",
                    "state": "success",
                    "description": "foobar",
                    "avatar_url": "",
                    "created_at": github_types.ISODateTimeType(
                        "2023-12-26 15:59:27.976414",
                    ),
                    "updated_at": github_types.ISODateTimeType(
                        "2023-12-26 15:59:27.976414",
                    ),
                },
            ),
        ],
    )
    ctxt._caches.pull_check_runs.set([])

    pull_attrs = condition_value_querier.PullRequest(ctxt)
    assert await pull_attrs.get_commit_message(template=template) == (
        title,
        message,
    )


@pytest.mark.parametrize(
    "body",
    [
        (
            """Hello world

# Commit Message
{{title}}

here is my message {{foobar}}
on two lines"""
        ),
        (
            """Hello world

# Commit Message
{{foobar}}

here is my message
on two lines"""
        ),
    ],
)
async def test_merge_commit_message_undefined(
    body: str,
    context_getter: conftest.ContextGetterFixture,
) -> None:
    ctxt = await context_getter(
        github_types.GitHubPullRequestNumber(43),
        body=body,
        title="My PR title",
    )
    with pytest.raises(condition_value_querier.RenderTemplateFailure) as x:
        await condition_value_querier.PullRequest(ctxt).get_commit_message()
    assert "foobar" in str(x.value)


@pytest.mark.parametrize(
    "body",
    [
        """Hello world

# Commit Message
{{title}}

here is my message {{ and broken template
""",
    ],
)
async def test_merge_commit_message_syntax_error(
    body: str,
    context_getter: conftest.ContextGetterFixture,
) -> None:
    ctxt = await context_getter(
        github_types.GitHubPullRequestNumber(43),
        body=body,
        title="My PR title",
    )
    with pytest.raises(condition_value_querier.RenderTemplateFailure):
        await condition_value_querier.PullRequest(ctxt).get_commit_message()


async def test_request_merge_without_method_merge_success(
    context_getter: conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
    redis_links: redis_utils.RedisLinks,
) -> None:
    context = await context_getter(123)

    executor = merge.MergeExecutor(
        ctxt=context,
        rule=mock.Mock(),
        config=mock.Mock(),
    )

    respx_mock.get(
        "https://api.github.com/repos/Mergifyio/mergify-engine/branches/main/protection",
    ).respond(200, json={})
    respx_mock.put(
        "https://api.github.com/repos/Mergifyio/mergify-engine/pulls/123/merge",
        json__merge_method="merge",
    ).respond(200)

    await executor._request_merge_without_method(
        ctxt=context,
        pull_merge_payload={},
        on_behalf=None,
    )

    assert await redis_links.cache.get("merge-method/0/0") == b"merge"


async def test_request_merge_without_method_rebase_success(
    context_getter: conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
    redis_links: redis_utils.RedisLinks,
) -> None:
    context = await context_getter(123)
    executor = merge.MergeExecutor(
        ctxt=context,
        rule=mock.Mock(),
        config=mock.Mock(),
    )

    respx_mock.get(
        "https://api.github.com/repos/Mergifyio/mergify-engine/branches/main/protection",
    ).respond(200, json={})
    respx_mock.put(
        "https://api.github.com/repos/Mergifyio/mergify-engine/pulls/123/merge",
        json__merge_method="merge",
    ).respond(405, json={"message": merge_base.FORBIDDEN_MERGE_COMMITS_MSG})
    respx_mock.put(
        "https://api.github.com/repos/Mergifyio/mergify-engine/pulls/123/merge",
        json__merge_method="squash",
    ).respond(405, json={"message": merge_base.FORBIDDEN_SQUASH_MERGE_MSG})
    respx_mock.put(
        "https://api.github.com/repos/Mergifyio/mergify-engine/pulls/123/merge",
        json__merge_method="rebase",
    ).respond(200)

    await executor._request_merge_without_method(
        ctxt=context,
        pull_merge_payload={},
        on_behalf=None,
    )

    assert await redis_links.cache.get("merge-method/0/0") == b"rebase"


async def test_request_merge_without_method_failure(
    context_getter: conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
) -> None:
    context = await context_getter(123)
    executor = merge.MergeExecutor(
        ctxt=context,
        rule=mock.Mock(),
        config=mock.Mock(),
    )

    respx_mock.get(
        "https://api.github.com/repos/Mergifyio/mergify-engine/branches/main/protection",
    ).respond(200, json={})
    respx_mock.put(
        "https://api.github.com/repos/Mergifyio/mergify-engine/pulls/123/merge",
        json__merge_method="merge",
    ).respond(405, json={"message": merge_base.FORBIDDEN_MERGE_COMMITS_MSG})
    respx_mock.put(
        "https://api.github.com/repos/Mergifyio/mergify-engine/pulls/123/merge",
        json__merge_method="squash",
    ).respond(405, json={"message": merge_base.FORBIDDEN_SQUASH_MERGE_MSG})
    respx_mock.put(
        "https://api.github.com/repos/Mergifyio/mergify-engine/pulls/123/merge",
        json__merge_method="rebase",
    ).respond(405, json={"message": merge_base.FORBIDDEN_REBASE_MERGE_MSG})

    with pytest.raises(http.HTTPClientSideError):
        await executor._request_merge_without_method(
            ctxt=context,
            pull_merge_payload={},
            on_behalf=None,
        )


async def test_request_merge_without_method_rebase_success_with_cache(
    context_getter: conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
    redis_links: redis_utils.RedisLinks,
) -> None:
    context = await context_getter(123)
    executor = merge.MergeExecutor(
        ctxt=context,
        rule=mock.Mock(),
        config=mock.Mock(),
    )
    await redis_links.cache.set("merge-method/0/0", "rebase")

    respx_mock.get(
        "https://api.github.com/repos/Mergifyio/mergify-engine/branches/main/protection",
    ).respond(200, json={})
    respx_mock.put(
        "https://api.github.com/repos/Mergifyio/mergify-engine/pulls/123/merge",
        json__merge_method="rebase",
    ).respond(200)

    await executor._request_merge_without_method(
        ctxt=context,
        pull_merge_payload={},
        on_behalf=None,
    )


async def test_request_merge_without_method_rebase_success_with_invalid_cache(
    context_getter: conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
    redis_links: redis_utils.RedisLinks,
) -> None:
    context = await context_getter(123)
    executor = merge.MergeExecutor(
        ctxt=context,
        rule=mock.Mock(),
        config=mock.Mock(),
    )
    await redis_links.cache.set("merge-method/0/0", "rebase")

    respx_mock.get(
        "https://api.github.com/repos/Mergifyio/mergify-engine/branches/main/protection",
    ).respond(200, json={})
    respx_mock.put(
        "https://api.github.com/repos/Mergifyio/mergify-engine/pulls/123/merge",
        json__merge_method="rebase",
    ).respond(405, json={"message": merge_base.FORBIDDEN_REBASE_MERGE_MSG})
    respx_mock.put(
        "https://api.github.com/repos/Mergifyio/mergify-engine/pulls/123/merge",
        json__merge_method="merge",
    ).respond(200)

    await executor._request_merge_without_method(
        ctxt=context,
        pull_merge_payload={},
        on_behalf=None,
    )


async def test_request_merge_without_method_rebase_success_with_linear_history_required(
    context_getter: conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
    redis_links: redis_utils.RedisLinks,
) -> None:
    context = await context_getter(123)
    executor = merge.MergeExecutor(
        ctxt=context,
        rule=mock.Mock(),
        config=mock.Mock(),
    )

    respx_mock.get(
        "https://api.github.com/repos/Mergifyio/mergify-engine/branches/main/protection",
    ).respond(200, json={"required_linear_history": {"enabled": True}})
    respx_mock.put(
        "https://api.github.com/repos/Mergifyio/mergify-engine/pulls/123/merge",
        json__merge_method="squash",
    ).respond(200)

    await executor._request_merge_without_method(
        ctxt=context,
        pull_merge_payload={},
        on_behalf=None,
    )

    assert await redis_links.cache.get("merge-method/0/0") == b"squash"
