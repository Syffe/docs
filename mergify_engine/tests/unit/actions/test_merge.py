import pytest

from mergify_engine import condition_value_querier
from mergify_engine import github_types
from mergify_engine.tests.unit import conftest


@pytest.mark.parametrize(
    "body, title, message, template",
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
        github_types.GitHubPullRequestNumber(43), body=body, title="My PR title"
    )
    ctxt.repository._caches.branch_protections[
        github_types.GitHubRefType("main")
    ] = None
    ctxt._caches.pull_statuses.set(
        [
            github_types.GitHubStatus(
                {
                    "target_url": "http://example.com",
                    "context": "my CI",
                    "state": "success",
                    "description": "foobar",
                    "avatar_url": "",
                }
            )
        ]
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
    body: str, context_getter: conftest.ContextGetterFixture
) -> None:
    ctxt = await context_getter(
        github_types.GitHubPullRequestNumber(43), body=body, title="My PR title"
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
"""
    ],
)
async def test_merge_commit_message_syntax_error(
    body: str, context_getter: conftest.ContextGetterFixture
) -> None:
    ctxt = await context_getter(
        github_types.GitHubPullRequestNumber(43), body=body, title="My PR title"
    )
    with pytest.raises(condition_value_querier.RenderTemplateFailure):
        await condition_value_querier.PullRequest(ctxt).get_commit_message()
