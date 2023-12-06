import anys
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
            "pull_requests": [expected_pull],
        }

    async def test_get_pull_request_summary(self) -> None:
        rules = f"""
pull_request_rules:
  - name: a lot of stuff
    conditions:
      - base={self.main_branch_name}
      - or:
        - schedule=MON-SUN 00:00-23:59
        - label=foobar
    actions:
      post_check:
        summary: "Did you check {{{{ author }}}}?"
        success_conditions:
          - "author=foobar"
          - "label=whatever"
          - "base={self.main_branch_name}"
      comment:
        message: "Welcome {{{{ author }}}}."
      edit:
        draft: True
      assign:
        users:
          - mergify-test1
queue_rules:
  - name: hotfix
    queue_conditions:
      - base={self.main_branch_name}
      - label=queue
    merge_conditions:
      - check-success=CI
"""
        await self.setup_repo(rules)
        p = await self.create_pr()

        owner = settings.TESTING_ORGANIZATION_NAME
        repo = self.RECORD_CONFIG["repository_name"]
        r = await self.admin_app.get(
            f"/v1/repos/{owner}/{repo}/pulls/{p['number']}/summary",
        )

        assert r.status_code == 200, r.json()
        assert len(r.json()["pull_request_rules"]) == 1
        pull_request_rule = r.json()["pull_request_rules"][0]
        assert pull_request_rule["name"] == "a lot of stuff"
        assert len(pull_request_rule["actions"]) == 4
        actions = pull_request_rule["actions"]
        assert actions["post_check"] == {
            "always_show": False,
            "success_conditions": anys.ANY_DICT,
            "neutral_conditions": None,
            "summary": anys.AnyFullmatch(r"Did you check .+\[bot\]\?"),
            "title": "'a lot of stuff' failed",
        }
        assert actions["comment"] == {
            "message": anys.AnyFullmatch(r"Welcome .+\[bot\]\."),
            "bot_account": None,
        }
        assert actions["edit"] == {"draft": True, "bot_account": None}
        assert actions["assign"] == {
            "users_to_add": ["mergify-test1"],
            "users_to_remove": [],
        }

        assert len(r.json()["queue_rules"]) == 1
        queue_rule = r.json()["queue_rules"][0]
        assert queue_rule["name"] == "hotfix"
        assert len(queue_rule["queue_conditions"]["subconditions"]) == 2
        assert len(queue_rule["merge_conditions"]["subconditions"]) == 1
