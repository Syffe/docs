from mergify_engine import config
from mergify_engine.clients import http


class AsyncDashboardSaasClient(http.AsyncClient):
    def __init__(self) -> None:
        super().__init__(
            base_url=config.SUBSCRIPTION_BASE_URL,
            headers={"Authorization": f"Bearer {config.ENGINE_TO_DASHBOARD_API_KEY}"},
            retry_stop_after_attempt=2,
            retry_exponential_multiplier=0.1,
        )


class AsyncDashboardOnPremiseClient(http.AsyncClient):
    def __init__(self) -> None:
        super().__init__(
            base_url=config.SUBSCRIPTION_BASE_URL,
            headers={"Authorization": f"token {config.SUBSCRIPTION_TOKEN}"},
        )
