import typing

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.clients import http


class AssociatedUsersAccount(typing.TypedDict):
    id: int


class AssociatedUser(typing.TypedDict):
    id: int
    membership: github_types.GitHubMembershipRole | None


class AssociatedUsers(typing.TypedDict):
    account: AssociatedUsersAccount
    associated_users: list[AssociatedUser]


class NoAssociatedUsersFoundError(Exception):
    pass


class AsyncShadowOfficeSaasClient(http.AsyncClient):
    def __init__(self) -> None:
        super().__init__(
            base_url=settings.SUBSCRIPTION_URL,
            headers={
                "Authorization": f"Bearer {settings.ENGINE_TO_SHADOW_OFFICE_API_KEY.get_secret_value()}",
            },
            retry_stop_after_attempt=2,
            retry_exponential_multiplier=0.1,
        )

    async def get_associated_users(
        self,
        login: github_types.GitHubLogin,
    ) -> list[AssociatedUser]:
        # Check if the login is an organization with billing system
        try:
            resp = await self.get(
                url=f"/engine/associated-users/{login}",
                follow_redirects=True,
            )
        except http.HTTPNotFoundError:
            raise NoAssociatedUsersFoundError(
                "User or Organization has no Mergify account",
            )

        data = typing.cast(AssociatedUsers, resp.json())
        # Try admin first
        return sorted(
            data["associated_users"],
            key=lambda x: x["membership"] != "admin",
        )


class AsyncShadowOfficeOnPremiseClient(http.AsyncClient):
    def __init__(self) -> None:
        if settings.SUBSCRIPTION_TOKEN is None:
            raise RuntimeError("settings.SUBSCRIPTION_TOKEN is None")
        super().__init__(
            base_url=settings.SUBSCRIPTION_URL,
            headers={
                "Authorization": f"token {settings.SUBSCRIPTION_TOKEN.get_secret_value()}",
            },
        )
