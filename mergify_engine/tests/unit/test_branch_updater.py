import pytest
import respx

from mergify_engine import branch_updater
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.tests.unit import conftest


@pytest.mark.respx(base_url=settings.GITHUB_REST_API_URL)
async def test_update_with_api_permission_error(
    context_getter: conftest.ContextGetterFixture,
    respx_mock: respx.MockRouter,
) -> None:
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))

    respx_mock.put("/repos/Mergifyio/mergify-engine/pulls/1/update-branch").respond(
        403,
        json={"message": "wow stop"},
    )

    with pytest.raises(branch_updater.BranchUpdateFailureError) as e:
        await branch_updater.update_with_api(ctxt)

    assert e.value.title == "Mergify doesn't have permission to update"
    assert e.value.message.startswith(
        "For security reasons, Mergify can't update this pull request. "
        "Try updating locally.\n"
        "GitHub response: wow stop",
    )
