import pytest

from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.tests.functional import base


class TestBranchUpdatePublic(base.FunctionalTestBase):
    @pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
    async def test_command_update(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "auto-rebase-on-conflict",
                    "conditions": ["conflict"],
                    "actions": {"comment": {"message": "nothing"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), files={"TESTING": "foobar"})
        p1 = await self.create_pr(files={"TESTING2": "foobar"})
        p2 = await self.create_pr(files={"TESTING3": "foobar"})
        await self.merge_pull(p1["number"])

        await self.create_comment_as_admin(p2["number"], "@mergifyio update")
        await self.run_engine()

        p2_updated = await self.wait_for_pull_request("synchronize")
        oldsha = p2["head"]["sha"]
        assert p2_updated["pull_request"]["commits"] == 2
        assert oldsha != p2_updated["pull_request"]["head"]["sha"]


# FIXME(sileht): This is not yet possible, due to GH restriction ...
# class TestBranchUpdatePrivate(TestBranchUpdatePublic):
#    REPO_NAME = "functional-testing-repo-private"
#    FORK_PERSONAL_TOKEN = config.ORG_USER_PERSONAL_TOKEN
#    SUBSCRIPTION_ACTIVE = True
