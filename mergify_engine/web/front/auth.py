import dataclasses
import typing

from authlib.integrations import starlette_client
import daiquiri
import fastapi
import imia
import sqlalchemy.ext.asyncio
import starsessions.session

from mergify_engine import config
from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.clients import dashboard
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.models import github_user
from mergify_engine.web.front import security


LOG = daiquiri.getLogger(__name__)


class TokenDictT(typing.TypedDict):
    access_token: str


oauth = starlette_client.OAuth()
oauth.register(
    "github",
    client_id=config.OAUTH_CLIENT_ID,
    client_secret=config.OAUTH_CLIENT_SECRET,
    api_base_url=config.GITHUB_REST_API_URL,
    authorize_url=f"{config.GITHUB_URL}/login/oauth/authorize",
    access_token_url=f"{config.GITHUB_URL}/login/oauth/access_token",
)


router = fastapi.APIRouter()


async def clear_authentication_data(request: fastapi.Request) -> None:
    request.session.clear()
    request.scope["auth"] = imia.UserToken(
        user=imia.AnonymousUser(), state=imia.LoginState.ANONYMOUS
    )


@dataclasses.dataclass
class OAuth2RedirectUrl:
    url: str


@router.get("/logout")
async def logout(request: fastapi.Request) -> fastapi.Response:
    await clear_authentication_data(request)
    return fastapi.Response(status_code=204)


@dataclasses.dataclass
class AuthRedirectUrl:
    url: str


# NOTE(sileht): react UI can't read redirect Location headers, so we need to send the url via a 200
@router.get("/authorize")
async def login_via_github(
    request: fastapi.Request,
    site_url: str | None = fastapi.Query(default=None),  # noqa: B008
) -> AuthRedirectUrl:
    if not site_url or site_url not in config.DASHBOARD_UI_SITE_URLS:
        # NOT a whitelisted domain, we just redirect to the default one.
        site_url = config.DASHBOARD_UI_SITE_URLS[0]

    starsessions.session.regenerate_session_id(request)
    await clear_authentication_data(request)
    rv = await oauth.github.create_authorization_url(f"{site_url}/auth/callback")
    await oauth.github.save_authorize_data(request, **rv)
    return AuthRedirectUrl(url=rv["url"])


async def create_or_update_user(
    request: fastapi.Request,
    session: sqlalchemy.ext.asyncio.AsyncSession,
    token: github_types.GitHubOAuthToken,
) -> github_user.GitHubUser:
    async with github.AsyncGithubInstallationClient(
        github.GithubTokenAuth(token)
    ) as client:
        try:
            user_data = typing.cast(
                github_types.GitHubAccount, await client.item("/user")
            )
        except http.HTTPUnauthorized:
            await clear_authentication_data(request)
            raise fastapi.HTTPException(401)

    user = await github_user.GitHubUser.create_or_update(
        session, user_data["id"], user_data["login"], token
    )

    if config.SAAS_MODE:
        # NOTE(sileht): This part is critical if we fail this call, future API calls token
        # /front/proxy/saas/ will fail as the accounts attached to this user will
        # TODO(sileht): This API call may call back the engine API to cleanup the token
        # for now the source of truth of tokens is still the dashboard backend until we
        # sync all user accounts on the engine side.
        async with dashboard.AsyncDashboardSaasClient() as client:
            await client.post(
                url="/engine/user-update",
                json={"user": user_data, "token": token},
            )
    return user


@router.get("/authorized")
async def auth_via_github(
    request: fastapi.Request,
    session: sqlalchemy.ext.asyncio.AsyncSession = fastapi.Depends(  # noqa: B008
        models.get_session
    ),
) -> fastapi.Response:
    try:
        token = await oauth.github.authorize_access_token(request)
    except starlette_client.OAuthError as e:
        await clear_authentication_data(request)
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
            else:
                raise fastapi.HTTPException(401)
        LOG.error(
            f"OAuth error error={e.error} description={e.description} uri={e.uri}",
            exc_info=True,
        )
        raise fastapi.HTTPException(401)

    user = await create_or_update_user(request, session, token["access_token"])
    await imia.login_user(request, user, "whatever")

    try:
        del request.session["sudo"]
    except KeyError:
        pass
    try:
        del request.session["sudoGrantedTo"]
    except KeyError:
        pass

    return fastapi.responses.Response(status_code=204)


@router.get("/setup")
async def auth_setup(
    request: fastapi.Request,
    current_user: github_user.GitHubUser = fastapi.Depends(  # noqa: B008
        security.get_current_user
    ),
    session: sqlalchemy.ext.asyncio.AsyncSession = fastapi.Depends(  # noqa: B008
        models.get_session
    ),
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
        async with github.AsyncGithubInstallationClient(
            github.GithubTokenAuth(current_user.oauth_access_token)
        ) as client:
            try:
                repos = typing.cast(
                    list[github_types.GitHubRepository],
                    await client.item(
                        f"/user/installations/{installation_id}/repositories?per_page=1"
                    ),
                )
            except http.HTTPNotFound:
                return AuthRedirectUrl("/github?new=true")

        return AuthRedirectUrl(f"/github/{repos[0]['owner']['login']}?new=true")
    elif setup_action == "request":
        return AuthRedirectUrl("/?request=true")

    LOG.warning("Unknown setup_action: %s", setup_action)
    return AuthRedirectUrl("/")
