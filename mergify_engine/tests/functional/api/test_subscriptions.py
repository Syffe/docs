from mergify_engine import config
from mergify_engine import yaml
from mergify_engine.tests.functional import base


class TestSubscriptionsApi(base.FunctionalTestBase):
    async def test_queue_freeze_subscription(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                    "batch_max_wait_time": "15 s",
                    "speculative_checks": 1,
                    "batch_size": 3,
                    "queue_branch_prefix": "urgent-",
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "speculative_checks": 2,
                    "batch_size": 2,
                    "batch_max_wait_time": "0 s",
                },
                {
                    "name": "low-priority",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "allow_inplace_checks": False,
                    "checks_timeout": "10 m",
                    "draft_bot_account": "mergify-test4",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue-urgent",
                    ],
                    "actions": {"queue": {"name": "urgent"}},
                },
                {
                    "name": "Merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
                {
                    "name": "Merge low",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue-low",
                    ],
                    "actions": {"queue": {"name": "low-priority"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
        )
        assert r.status_code == 402

    async def test_eventlogs_subscription(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "hello",
                    "conditions": [f"base={self.main_branch_name}", "label=auto-merge"],
                    "actions": {
                        "comment": {
                            "message": "Hello!",
                        },
                    },
                },
                {
                    "name": "mergeit",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "assign": {"users": ["mergify-test1"]},
                        "label": {
                            "add": ["need-review"],
                            "remove": ["auto-merge"],
                        },
                        "merge": {},
                    },
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "auto-merge")
        await self.run_engine()

        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p1['number']}/events",
        )
        assert r.status_code == 402
