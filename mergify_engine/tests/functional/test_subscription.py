import typing
from unittest import mock

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine.tests.functional import base


class TestSubscription(base.FunctionalTestBase):
    async def test_subscription(self) -> None:
        async def fake_subscription(
            _redis_cache: redis_utils.RedisCache,
            _owner_id: github_types.GitHubAccountIdType,
        ) -> subscription.Subscription:
            if self.SUBSCRIPTION_ACTIVE:
                features = frozenset(
                    getattr(subscription.Features, f)
                    for f in subscription.Features.__members__
                )
                all_features = [
                    typing.cast(subscription.FeaturesLiteralT, f.value)
                    for f in subscription.Features
                ]
            else:
                features = frozenset([])
                all_features = []

            return subscription.Subscription(
                self.redis_links.cache,
                settings.TESTING_ORGANIZATION_ID,
                "Abuse",
                features,
                all_features,
            )

        patcher = mock.patch(
            "mergify_engine.subscription.Subscription._retrieve_subscription_from_db",
            side_effect=fake_subscription,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        await self.setup_repo()
        await self.create_pr()
        await self.run_engine()

        check_run = await self.wait_for_check_run(conclusion="failure")

        assert (
            check_run["check_run"]["output"]["summary"]
            == "⚠ The [subscription](http://localhost:3000/github/mergifyio-testing/subscription) needs to be updated to enable this feature."
        )
        assert (
            check_run["check_run"]["output"]["title"]
            == "Cannot use Mergify on a public repository"
        )
