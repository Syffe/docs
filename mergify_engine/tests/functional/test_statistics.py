import msgpack

from mergify_engine import yaml
from mergify_engine.queue import statistics
from mergify_engine.tests.functional import base


class TestStatisticsRedis(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_statistics_format_in_redis(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                    "speculative_checks": 5,
                    "batch_size": 2,
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")

        await self.run_engine()

        await self.wait_for("pull_request", {"action": "opened"})

        await self.run_engine()

        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})

        redis_repo_key = statistics._get_repository_key(
            self.subscription.owner_id, self.RECORD_CONFIG["repository_id"]
        )
        time_to_merge_key = f"{redis_repo_key}/time_to_merge"
        assert await self.redis_links.stats.xlen(time_to_merge_key) == 2

        bdata = await self.redis_links.stats.xrange(time_to_merge_key, min="-", max="+")
        for _, raw in bdata:
            statistics.TimeToMerge(**msgpack.unpackb(raw[b"data"], timestamp=3))
