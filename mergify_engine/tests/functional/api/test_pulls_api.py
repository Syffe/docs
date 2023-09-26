import pydantic

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.tests.functional import base


class TestPullsApi(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_pulls_api_basic(self) -> None:
        await self.setup_repo()

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        await self.add_label(p1["number"], "hellothere")
        await self.add_label(p2["number"], "generalkenobi")

        # pydantic removes all the extra fields which are not present in a typeddict
        # So we need to use the typeadapter to be able to validate that the pull,
        # which we will retrieve directly from GitHub, is the same as the one
        # we get in the output of the endpoint.
        ta = pydantic.TypeAdapter(github_types.GitHubPullRequest)

        p1_full = await self.get_pull(p1["number"])
        expected_pull = ta.validate_python(p1_full)

        resp = await self.admin_app.post(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls",
            json=["label=hellothere"],
        )

        assert resp.status_code == 200

        assert resp.json() == {
            "size": 1,
            "per_page": 10,
            "total": 1,
            "pull_requests": [expected_pull],
        }
