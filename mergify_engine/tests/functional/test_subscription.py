from unittest import mock

from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine.tests.functional import base


class TestSubscription(base.FunctionalTestBase):
    async def test_subscription(self) -> None:
        self.register_mock(
            mock.patch.object(
                subscription.Subscription,
                "_retrieve_subscription_from_db",
                return_value=subscription.Subscription(
                    self.redis_links.cache,
                    settings.TESTING_ORGANIZATION_ID,
                    "Abuse",
                    frozenset([subscription.Features.PRIVATE_REPOSITORY]),
                    [],
                ),
            ),
        )

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
