import pytest

from mergify_engine import config
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine.engine import actions_runner
from mergify_engine.tests.unit import conftest


@pytest.mark.parametrize(
    "merged_by,merged_by_id,raw_config,result",
    [
        (
            config.BOT_USER_LOGIN,
            config.BOT_USER_ID,
            """
pull_request_rules:
 - name: Automatic merge on approval
   conditions:
     - and:
       - "-draft"
       - "author=contributor"
   actions:
     merge:
       method: merge
""",
            "",
        ),
        (
            "foobar",
            github_types.GitHubAccountIdType(1),
            """
queue_rules:
  - name: foo
    conditions: []
pull_request_rules:
 - name: Automatic queue on approval
   conditions:
     - and:
       - "-draft"
       - "author=contributor"
   actions:
     queue:
       name: foo
        """,
            "⚠️ The pull request has been merged by @foobar\n\n",
        ),
        (
            config.BOT_USER_LOGIN,
            config.BOT_USER_ID,
            """
pull_request_rules:
 - name: Automatic queue on approval
   conditions:
     - and:
       - "-draft"
       - "author=contributor"
   actions:
     delete_head_branch:
        """,
            "⚠️ The pull request has been closed by GitHub because its commits are also part of another pull request\n\n",
        ),
    ],
)
async def test_get_already_merged_summary(
    merged_by: github_types.GitHubLogin,
    merged_by_id: github_types.GitHubAccountIdType,
    raw_config: str,
    result: str,
    context_getter: conftest.ContextGetterFixture,
) -> None:
    ctxt = await context_getter(
        github_types.GitHubPullRequestNumber(1),
        merged=True,
        merged_by=github_types.GitHubAccount(
            {
                "id": merged_by_id,
                "login": merged_by,
                "type": "User",
                "avatar_url": "",
            }
        ),
    )
    ctxt.repository._caches.branch_protections[
        github_types.GitHubRefType("main")
    ] = None

    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path="whatever",
        decoded_content=raw_config,
    )

    config = rules.get_mergify_config(file)
    match = await config["pull_request_rules"].get_pull_request_rule(ctxt)
    assert result == await actions_runner.get_already_merged_summary(ctxt, match)
