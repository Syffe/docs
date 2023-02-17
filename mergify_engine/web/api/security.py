from collections import abc
import typing

import daiquiri
import fastapi
import sentry_sdk

from mergify_engine import config
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.dashboard import application as application_mod
from mergify_engine.dashboard import subscription
from mergify_engine.models import github_user
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web import redis


LOG = daiquiri.getLogger(__name__)

ScopeT = typing.NewType("ScopeT", str)


class HttpAuth(typing.NamedTuple):
    installation: github_types.GitHubInstallation
    auth: github.GithubAppInstallationAuth | github.GithubTokenAuth
    actor: github.Actor


class ApplicationAuth(fastapi.security.http.HTTPBearer):
    async def __call__(  # type: ignore[override]
        self,
        request: fastapi.Request,
        redis_links: redis_utils.RedisLinks = fastapi.Depends(  # noqa: B008
            redis.get_redis_links
        ),
    ) -> application_mod.Application | None:
        credentials = await super().__call__(request)
        if credentials is None:
            if self.auto_error:
                # Not really possible it has been raised earlier but I please mypy
                raise fastapi.HTTPException(status_code=403)
            else:
                return None

        scope: github_types.GitHubLogin | None = request.path_params.get("owner")
        api_access_key = credentials.credentials[: config.API_ACCESS_KEY_LEN]
        api_secret_key = credentials.credentials[config.API_ACCESS_KEY_LEN :]
        try:
            app = await application_mod.Application.get(
                redis_links.cache, api_access_key, api_secret_key, scope
            )
        except application_mod.ApplicationUserNotFound:
            if self.auto_error:
                raise fastapi.HTTPException(status_code=403)
            else:
                return None

        # Seatbelt
        current_scope = (
            None if app.account_scope is None else app.account_scope["login"]
        )
        if scope is not None and (
            current_scope is None or current_scope.lower() != scope.lower()
        ):
            LOG.error(
                "got application with wrong scope",
                expected_scope=scope,
                current_scope=current_scope,
            )
            if self.auto_error:
                raise fastapi.HTTPException(status_code=403)
            else:
                return None
        return app


get_application = ApplicationAuth()
get_optional_application = ApplicationAuth(auto_error=False)


def build_actor(
    auth_method: application_mod.Application | github_user.GitHubUser,
) -> github.Actor:
    if isinstance(auth_method, application_mod.Application):
        return github.Actor(
            type="application",
            id=auth_method.id,
            name=auth_method.name,
        )
    else:
        return github.Actor(
            type="user",
            id=auth_method.id,
            name=auth_method.login,
        )


async def get_http_auth(
    request: fastapi.Request,
    owner: github_types.GitHubLogin = fastapi.Path(  # noqa: B008
        ..., description="The owner of the repository"
    ),
    application: application_mod.Application
    | None = fastapi.Security(get_optional_application),  # noqa: B008
) -> HttpAuth:
    auth: github.GithubAppInstallationAuth | github.GithubTokenAuth

    if application is not None:
        installation_json = await github.get_installation_from_login(owner)
        # Authenticated by token
        if (
            application.account_scope is not None
            and application.account_scope["id"] != installation_json["account"]["id"]
        ):
            raise fastapi.HTTPException(status_code=403)
        auth = github.GithubAppInstallationAuth(installation_json)
        actor = build_actor(auth_method=application)

    elif "auth" in request.scope and request.auth and request.auth.is_authenticated:
        # Authenticated by cookie session
        user = typing.cast(github_user.GitHubUser, request.auth.user)
        installation_json = await github.get_installation_from_login(owner)
        auth = github.GithubTokenAuth(user.oauth_access_token)
        actor = build_actor(auth_method=user)
    else:
        raise fastapi.HTTPException(403)

    return HttpAuth(
        installation=installation_json,
        auth=auth,
        actor=actor,
    )


async def get_repository_context(
    request: fastapi.Request,
    owner: github_types.GitHubLogin = fastapi.Path(  # noqa: B008
        ..., description="The owner of the repository"
    ),
    repository: github_types.GitHubRepositoryName = fastapi.Path(  # noqa: B008
        ..., description="The name of the repository"
    ),
    redis_links: redis_utils.RedisLinks = fastapi.Depends(  # noqa: B008
        redis.get_redis_links
    ),
    application: application_mod.Application
    | None = fastapi.Security(get_optional_application),  # noqa: B008
) -> abc.AsyncGenerator[context.Repository, None]:
    installation_json, auth, actor = await get_http_auth(request, owner, application)
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

        # NOTE(sileht): Since this method is used as fastapi Depends only, it's safe to set this
        # for the ongoing http request
        sentry_sdk.set_user({"username": repository_ctxt.installation.owner_login})
        sentry_sdk.set_tag("gh_owner", repository_ctxt.installation.owner_login)
        sentry_sdk.set_tag("gh_repo", repository_ctxt.repo["name"])

        yield repository_ctxt


require_authentication = get_repository_context


async def check_subscription_feature_queue_freeze(
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        get_repository_context
    ),
) -> None:
    if not repository_ctxt.installation.subscription.has_feature(
        subscription.Features.QUEUE_FREEZE
    ):
        raise fastapi.HTTPException(
            status_code=402,
            detail="⚠ The subscription needs to be upgraded to enable the `queue_freeze` feature.",
        )


async def check_subscription_feature_eventlogs(
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        get_repository_context
    ),
) -> None:
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
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        get_repository_context
    ),
) -> None:
    if repository_ctxt.installation.subscription.has_feature(
        subscription.Features.MERGE_QUEUE_STATS
    ):
        return

    raise fastapi.HTTPException(
        status_code=402,
        detail="⚠ The subscription needs to be upgraded to enable the `merge_queue_stats` feature.",
    )


async def get_queue_rules(
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        get_repository_context
    ),
) -> qr_config.QueueRules:
    try:
        mergify_config = await repository_ctxt.get_mergify_config()
    except mergify_conf.InvalidRules:
        raise fastapi.HTTPException(
            status_code=422,
            detail="The configuration file is invalid.",
        )
    return mergify_config["queue_rules"]
