from collections import abc
import typing

import daiquiri
import fastapi
import sentry_sdk

from mergify_engine import context
from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.config import types
from mergify_engine.dashboard import subscription
from mergify_engine.models import application_keys
from mergify_engine.models import github_account
from mergify_engine.models import github_user
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web import redis


LOG = daiquiri.getLogger(__name__)

ScopeT = typing.NewType("ScopeT", str)


class _HttpAuth(typing.NamedTuple):
    installation: github_types.GitHubInstallation
    auth: github.GithubAppInstallationAuth | github.GithubTokenAuth
    actor: github.Actor


class ApplicationAuth(fastapi.security.http.HTTPBearer):
    def __init__(
        self,
        *,
        bearerFormat: str | None = None,
        scheme_name: str | None = None,
        description: str | None = None,
        auto_error: bool = True,
        verify_scope: bool = True,
    ):
        self.verify_scope = verify_scope
        super().__init__(
            bearerFormat=bearerFormat,
            scheme_name=scheme_name,
            description=description,
            auto_error=auto_error,
        )

    async def __call__(  # type: ignore[override]
        self,
        request: fastapi.Request,
        redis_links: redis.RedisLinks,
        session: database.Session,
    ) -> application_keys.ApplicationKey | None:
        credentials = await super().__call__(request)
        if credentials is None:
            if self.auto_error:
                # Not really possible it has been raised earlier but I please mypy
                raise fastapi.HTTPException(status_code=403)

            return None

        api_access_key = credentials.credentials[: types.API_ACCESS_KEY_LEN]
        api_secret_key = credentials.credentials[types.API_ACCESS_KEY_LEN :]

        app = await application_keys.ApplicationKey.get_by_key(
            session, api_access_key, api_secret_key
        )
        if app is None:
            data = settings.APPLICATION_APIKEYS.get(api_access_key)
            if data and data["api_secret_key"] == api_secret_key:
                app = application_keys.ApplicationKey(
                    id=0,
                    name="on-premise-app-from-env",
                    api_access_key=api_access_key,
                    api_secret_key=api_secret_key,
                    github_account=github_account.GitHubAccount(
                        id=github_types.GitHubAccountIdType(data["account_id"]),
                        login=github_types.GitHubLogin(data["account_login"]),
                    ),
                    created_at=date.utcnow(),
                )

        if app is None:
            if self.auto_error:
                raise fastapi.HTTPException(status_code=403)
            return None

        if self.verify_scope:
            scope: github_types.GitHubLogin | None = request.path_params.get("owner")
            if scope is None:
                raise RuntimeError(
                    "verify_scope is true, but url doesn't have owner variable"
                )
            # Seatbelt
            if app.github_account.login.lower() != scope.lower():
                if self.auto_error:
                    raise fastapi.HTTPException(status_code=403)
                return None

        return app


get_application = ApplicationAuth()
get_application_without_scope_verification = ApplicationAuth(verify_scope=False)
get_optional_application = ApplicationAuth(auto_error=False)

LoggedApplication = typing.Annotated[
    application_keys.ApplicationKey | None, fastapi.Security(get_optional_application)
]


def build_actor(
    auth_method: application_keys.ApplicationKey | github_user.GitHubUser,
) -> github.Actor:
    if isinstance(auth_method, application_keys.ApplicationKey):
        return github.Actor(
            type="application",
            id=auth_method.id,
            name=auth_method.name,
        )

    return github.Actor(
        type="user",
        id=auth_method.id,
        name=auth_method.login,
    )


async def get_logged_user(request: fastapi.Request) -> github_user.GitHubUser | None:
    if "auth" in request.scope and request.auth and request.auth.is_authenticated:
        return typing.cast(github_user.GitHubUser, request.auth.user)
    return None


LoggedUser = typing.Annotated[
    github_user.GitHubUser | None, fastapi.Security(get_logged_user)
]


async def get_http_auth(
    owner: typing.Annotated[
        github_types.GitHubLogin,
        fastapi.Path(description="The owner of the repository"),
    ],
    application: LoggedApplication,
    logged_user: LoggedUser,
) -> _HttpAuth:
    auth: github.GithubAppInstallationAuth | github.GithubTokenAuth
    if application is not None:
        installation_json = await github.get_installation_from_login(owner)
        # Authenticated by token
        if application.github_account.id != installation_json["account"]["id"]:
            raise fastapi.HTTPException(status_code=403)
        auth = github.GithubAppInstallationAuth(installation_json)
        actor = build_actor(auth_method=application)

    elif logged_user is not None:
        # Authenticated by cookie session
        installation_json = await github.get_installation_from_login(owner)
        auth = github.GithubTokenAuth(logged_user.oauth_access_token)
        actor = build_actor(auth_method=logged_user)
    else:
        raise fastapi.HTTPException(403)

    return _HttpAuth(
        installation=installation_json,
        auth=auth,
        actor=actor,
    )


HttpAuth = typing.Annotated[_HttpAuth, fastapi.Depends(get_http_auth)]


async def get_repository_context(
    owner: typing.Annotated[
        github_types.GitHubLogin,
        fastapi.Path(description="The owner of the repository"),
    ],
    repository: typing.Annotated[
        github_types.GitHubRepositoryName,
        fastapi.Path(description="The name of the repository"),
    ],
    redis_links: redis.RedisLinks,
    application: LoggedApplication,
    logged_user: LoggedUser,
) -> abc.AsyncGenerator[context.Repository, None]:
    installation_json, auth, actor = await get_http_auth(
        owner, application, logged_user
    )

    async with github.AsyncGithubInstallationClient(
        auth,
    ) as client:
        try:
            # Check this token has access to this repository
            repo = typing.cast(
                github_types.GitHubRepository,
                await client.item(f"/repos/{owner}/{repository}"),
            )
        except (http.HTTPNotFound, http.HTTPForbidden, http.HTTPUnauthorized):
            raise fastapi.HTTPException(status_code=404)

        sub = await subscription.Subscription.get_subscription(
            redis_links.cache, repo["owner"]["id"]
        )
        if sub.has_feature(subscription.Features.PRIVATE_REPOSITORY):
            client.enable_extra_metrics()

        installation = context.Installation(installation_json, sub, client, redis_links)

        repository_ctxt = installation.get_repository_from_github_data(repo)

        if logged_user is not None and not repo["private"]:
            try:
                permission = await repository_ctxt.get_user_permission(
                    logged_user.to_github_account()
                )
            except http.HTTPForbidden:
                raise fastapi.HTTPException(status_code=403)

            if permission == github_types.GitHubRepositoryPermission.NONE:
                raise fastapi.HTTPException(status_code=403)

        # NOTE(sileht): Since this method is used as fastapi Depends only, it's safe to set this
        # for the ongoing http request
        sentry_sdk.set_user({"username": repository_ctxt.installation.owner_login})
        sentry_sdk.set_tag("gh_owner", repository_ctxt.installation.owner_login)
        sentry_sdk.set_tag("gh_repo", repository_ctxt.repo["name"])

        yield repository_ctxt


require_authentication = get_repository_context

Repository = typing.Annotated[
    context.Repository, fastapi.Depends(get_repository_context)
]


async def check_logged_user_has_write_access(
    application: LoggedApplication,
    logged_user: LoggedUser,
    repository_ctxt: Repository,
) -> None:
    if application is not None:
        # Application keys has no scope yet, just allow everything
        return

    if logged_user is None:
        raise fastapi.HTTPException(status_code=403)

    try:
        permission = await repository_ctxt.get_user_permission(
            logged_user.to_github_account()
        )
    except http.HTTPForbidden:
        raise fastapi.HTTPException(status_code=403)

    if permission < github_types.GitHubRepositoryPermission.WRITE:
        raise fastapi.HTTPException(status_code=403)


async def check_subscription_feature_queue_freeze(repository_ctxt: Repository) -> None:
    if not repository_ctxt.installation.subscription.has_feature(
        subscription.Features.QUEUE_FREEZE
    ):
        raise fastapi.HTTPException(
            status_code=402,
            detail="⚠ The subscription needs to be upgraded to enable the `queue_freeze` feature.",
        )


async def check_subscription_feature_eventlogs(repository_ctxt: Repository) -> None:
    if repository_ctxt.installation.subscription.has_feature(
        subscription.Features.EVENTLOGS_LONG
    ) or repository_ctxt.installation.subscription.has_feature(
        subscription.Features.EVENTLOGS_SHORT
    ):
        return

    raise fastapi.HTTPException(
        status_code=402,
        detail="⚠ The subscription needs to be upgraded to enable the `eventlogs` feature.",
    )


async def check_subscription_feature_merge_queue_stats(
    repository_ctxt: Repository,
) -> None:
    if repository_ctxt.installation.subscription.has_feature(
        subscription.Features.MERGE_QUEUE_STATS
    ):
        return

    raise fastapi.HTTPException(
        status_code=402,
        detail="⚠ The subscription needs to be upgraded to enable the `merge_queue_stats` feature.",
    )


async def get_queue_rules(repository_ctxt: Repository) -> qr_config.QueueRules:
    try:
        mergify_config = await repository_ctxt.get_mergify_config()
    except mergify_conf.InvalidRules:
        raise fastapi.HTTPException(
            status_code=422,
            detail="The configuration file is invalid.",
        )
    return mergify_config["queue_rules"]


QueueRules = typing.Annotated[qr_config.QueueRules, fastapi.Depends(get_queue_rules)]


async def get_partition_rules(
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        get_repository_context
    ),
) -> partr_config.PartitionRules:
    try:
        mergify_config = await repository_ctxt.get_mergify_config()
    except mergify_conf.InvalidRules:
        raise fastapi.HTTPException(
            status_code=422,
            detail="The configuration file is invalid.",
        )
    return mergify_config["partition_rules"]


PartitionRules = typing.Annotated[
    partr_config.PartitionRules, fastapi.Depends(get_partition_rules)
]
