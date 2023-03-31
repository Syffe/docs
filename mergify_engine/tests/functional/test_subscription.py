from unittest import mock

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.dashboard import subscription
from mergify_engine.tests.functional import base


class TestSubscription(base.FunctionalTestBase):
    async def test_subscription(self) -> None:
        async def fake_subscription(
            redis_cache: redis_utils.RedisCache,
            owner_id: github_types.GitHubAccountIdType,
        ) -> subscription.Subscription:
            return subscription.Subscription(
                self.redis_links.cache,
                settings.TESTING_ORGANIZATION_ID,
                "Abuse",
                frozenset(
                    getattr(subscription.Features, f)
                    for f in subscription.Features.__members__
                )
                if self.SUBSCRIPTION_ACTIVE
                else frozenset([]),
            )

        patcher = mock.patch(
            "mergify_engine.dashboard.subscription.Subscription._retrieve_subscription_from_db",
            side_effect=fake_subscription,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        await self.setup_repo()
        await self.create_pr()
        await self.run_engine()

        check_run = await self.wait_for_check_run(conclusion="failure")

        assert (
            "⚠ The [subscription](https://dashboard.mergify.com/github/mergifyio-testing/subscription) needs to be updated to enable this feature."
            == check_run["check_run"]["output"]["summary"]
        )
        assert (
            "Cannot use Mergify on a public repository"
            == check_run["check_run"]["output"]["title"]
        )
