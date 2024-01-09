import pytest

from mergify_engine import context
from mergify_engine.tests.functional import base
from mergify_engine.yaml import yaml


@pytest.mark.usefixtures("_enable_github_in_postgres")
class TestEngineWithPostgres(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_events_not_handled_before_postgres(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Queue",
                    "conditions": [
                        "label=queue",
                    ],
                    "actions": {"queue": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        # p1 shouldn't have been handled yet because the github-in-postgres
        # service wasn't enabled yet
        ctxt_p1 = context.Context(self.repository_ctxt, p1, [])
        assert len(await ctxt_p1.pull_check_runs) == 0
        assert len(await ctxt_p1.pull_engine_check_runs) == 0

        await self.run_engine({"github-in-postgres"})

        await self.wait_for_pull_request("opened")
