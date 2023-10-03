import dataclasses
import typing

from authlib.integrations import httpx_client
from authlib.integrations import starlette_client
import daiquiri
import fastapi
import httpx
import imia
import sqlalchemy.ext.asyncio
import starsessions.session

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.clients import shadow_office
from mergify_engine.models.github import user as github_user
from mergify_engine.web.front import security


LOG = daiquiri.getLogger(__name__)


class TokenDictT(typing.TypedDict):
    access_token: str


class CustomAsyncOAuth2Client(httpx_client.AsyncOAuth2Client):  # type: ignore[misc]
    retry_stop_after_attempt = 2

    async def send(
        self, request: httpx.Request, *args: typing.Any, **kwargs: typing.Any
    ) -> httpx.Response:
        kwargs["retry_stop_after_attempt"] = self.retry_stop_after_attempt
        return await http.retry_async_httpx_send(super().send, request, *args, **kwargs)


class CustomStarletteOAuth2App(
    starlette_client.StarletteOAuth2App  # type: ignore[misc]
):
    client_cls = CustomAsyncOAuth2Client


class CustomStarletteOAuth(starlette_client.OAuth):  # type: ignore[misc]
    oauth2_client_cls = CustomStarletteOAuth2App


oauth = CustomStarletteOAuth()
oauth.register(
    "github",
    client_id=settings.GITHUB_OAUTH_CLIENT_ID,
    client_secret=settings.GITHUB_OAUTH_CLIENT_SECRET.get_secret_value(),
    api_base_url=settings.GITHUB_REST_API_URL,
    authorize_url=f"{settings.GITHUB_URL}/login/oauth/authorize",
    access_token_url=f"{settings.GITHUB_URL}/login/oauth/access_token",
)


router = fastapi.APIRouter(tags=["front"])


async def clear_session_and_auth(request: fastapi.Request) -> None:
    request.session.clear()
    session_handler = starsessions.session.get_session_handler(request)
    await session_handler.destroy()
    session_handler.regenerate_id()
    request.scope["auth"] = imia.UserToken(
        user=imia.AnonymousUser(), state=imia.LoginState.ANONYMOUS
    )


@dataclasses.dataclass
class OAuth2RedirectUrl:
    url: str


@router.get("/logout")
async def logout(request: fastapi.Request) -> fastapi.Response:
    await clear_session_and_auth(request)
    return fastapi.Response(status_code=204)


@dataclasses.dataclass
class AuthRedirectUrl:
    url: str


# NOTE(sileht): react UI can't read redirect Location headers, so we need to send the url via a 200
@router.get("/authorize")
async def login_via_github(
    request: fastapi.Request,
    site_url: typing.Annotated[str | None, fastapi.Query()] = None,
) -> AuthRedirectUrl:
    if not site_url or site_url != settings.DASHBOARD_UI_FRONT_URL:
        # NOT a whitelisted domain, we just redirect to the default one.
        if site_url:
            LOG.warning(
                "got authorize request with unexpected site_url value",
                unexpected_url=site_url,
            )
        site_url = settings.DASHBOARD_UI_FRONT_URL

    # NOTE(sileht): logout first to start with a new session and session id
    await clear_session_and_auth(request)

    rv = await oauth.github.create_authorization_url(f"{site_url}/auth/callback")
    await oauth.github.save_authorize_data(request, **rv)
    return AuthRedirectUrl(url=rv["url"])


async def create_or_update_user(
    request: fastapi.Request,
    session: sqlalchemy.ext.asyncio.AsyncSession,
    token: github_types.GitHubOAuthToken,
) -> github_user.GitHubUser:
    async with github.AsyncGitHubInstallationClient(
        github.GitHubTokenAuth(token)
    ) as client:
        try:
            user_data = typing.cast(
                github_types.GitHubAccount, await client.item("/user")
            )
        except http.HTTPUnauthorized:
            await clear_session_and_auth(request)
            raise fastapi.HTTPException(401)

    user = await github_user.GitHubUser.create_or_update(
        session, user_data["id"], user_data["login"], token
    )

    if settings.SAAS_MODE:
        # NOTE(sileht): This part is critical if we fail this call, future API calls token
        # /front/proxy/saas/ will fail as the accounts attached to this user will
        # TODO(sileht): This API call may call back the engine API to cleanup the token
        # for now the source of truth of tokens is still the dashboard backend until we
        # sync all user accounts on the engine side.
        async with shadow_office.AsyncShadowOfficeSaasClient() as client:
            await client.post(
                url="/engine/user-update",
                json={"user": user_data, "token": token},
            )
    return user


@router.get("/authorized")
async def auth_via_github(
    request: fastapi.Request,
    session: database.Session,
) -> fastapi.Response:
    try:
        token = await oauth.github.authorize_access_token(request)
    except starlette_client.OAuthError as e:
        await clear_session_and_auth(request)
        if "access_denied" == e.error:
            raise fastapi.HTTPException(403)
        if e.error in (
            "bad_verification_code",
            "mismatching_state",
            "redirect_uri_mismatch",
        ):
            # old verification code, just retry
            LOG.warning("OAuth2 failed, retrying", exc_info=True)
            if e.error == "redirect_uri_mismatch":
                raise fastapi.HTTPException(410)
            raise fastapi.HTTPException(401)

        LOG.error(
            "OAuth error error=%s description=%s uri=%s",
            e.error,
            e.description,
            e.uri,
            exc_info=True,
        )
        raise fastapi.HTTPException(401)

    # NOTE(sileht): clear the session to avoid any oauth state leakage and
    # session id reuse
    await clear_session_and_auth(request)

    user = await create_or_update_user(request, session, token["access_token"])
    await imia.login_user(request, user, "whatever")

    return fastapi.responses.Response(status_code=204)


@router.get("/setup")
async def auth_setup(
    request: fastapi.Request,
    current_user: security.CurrentUser,
    session: database.Session,
    setup_action: str | None = None,
    installation_id: int | None = None,
) -> AuthRedirectUrl:
    if setup_action == "install":
        if installation_id is None:
            raise fastapi.HTTPException(400, "No installation_id passed")

        # We need to sync subscription
        await create_or_update_user(request, session, current_user.oauth_access_token)

        # NOTE(sileht): didn't found a better way to get the owner id from the
        # installation id with listing all installations
        async with github.AsyncGitHubInstallationClient(
            github.GitHubTokenAuth(current_user.oauth_access_token)
        ) as client:
            try:
                data = typing.cast(
                    github_types.GitHubRepositoryList,
                    await client.item(
                        f"/user/installations/{installation_id}/repositories?per_page=1"
                    ),
                )
            except http.HTTPNotFound:
                return AuthRedirectUrl("/github?new=true")

        if len(data["repositories"]) < 1:
            return AuthRedirectUrl("/github?new=true")

        return AuthRedirectUrl(
            f"/github/{data['repositories'][0]['owner']['login']}?new=true"
        )

    if setup_action == "request":
        return AuthRedirectUrl("/github?request=true")

    LOG.warning("Unknown setup_action: %s", setup_action)
    return AuthRedirectUrl("/")
