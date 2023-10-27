from collections import abc
import datetime
import typing

import daiquiri
import fastapi
from pydantic import functional_validators
import sentry_sdk

from mergify_engine import context
from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.config import types
from mergify_engine.models import application_keys
from mergify_engine.models import github as gh_models
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web import redis
from mergify_engine.web import utils


LOG = daiquiri.getLogger(__name__)

ScopeT = typing.NewType("ScopeT", str)

AUTH_CACHE_EXPIRE = datetime.timedelta(days=7)


def NoStartingEndingHyphen(v: str) -> str:
    # FIXME(sileht): pydantic does not support look-ahead/look-behind regex
    # so we can't check for starting and ending hyphen with a readable regex
    assert not (v.startswith("-") or v.endswith("-"))
    return v


RepositoryOwnerLogin = typing.Annotated[
    github_types.GitHubLogin,
    fastapi.Path(
        description="The owner of the repository",
        pattern=r"^[a-zA-Z0-9\-]+$",
        min_length=1,
        max_length=40,
    ),
    functional_validators.AfterValidator(NoStartingEndingHyphen),
]

_RepositoryNamePathConfig = fastapi.Path(
    description="The name of the repository", pattern=r"^[\w\-\.]+$", min_length=1
)

RepositoryName = typing.Annotated[
    github_types.GitHubRepositoryName,
    _RepositoryNamePathConfig,
]

OptionalRepositoryName = typing.Annotated[
    github_types.GitHubRepositoryName | None,
    _RepositoryNamePathConfig,
]


class _AuthenticationActor(typing.NamedTuple):
    installation: github_types.GitHubInstallation
    github_client_auth: github.GitHubAppInstallationAuth | github.GitHubTokenAuth
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
                    github_account_id=github_types.GitHubAccountIdType(
                        data["account_id"]
                    ),
                    github_account=gh_models.GitHubAccount(
                        id=github_types.GitHubAccountIdType(data["account_id"]),
                        login=github_types.GitHubLogin(data["account_login"]),
                        avatar_url=gh_models.GitHubAccount.build_avatar_url(
                            data["account_id"]
                        ),
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
    auth_method: application_keys.ApplicationKey | gh_models.GitHubUser,
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


async def get_logged_user(request: fastapi.Request) -> gh_models.GitHubUser | None:
    if "auth" in request.scope and request.auth and request.auth.is_authenticated:
        return typing.cast(gh_models.GitHubUser, request.auth.user)
    return None


LoggedUser = typing.Annotated[
    gh_models.GitHubUser | None, fastapi.Security(get_logged_user)
]


async def _get_authenticated_actor(
    owner: RepositoryOwnerLogin,
    repository: OptionalRepositoryName,
    application: LoggedApplication,
    logged_user: LoggedUser,
) -> _AuthenticationActor:
    github_client_auth: github.GitHubAppInstallationAuth | github.GitHubTokenAuth

    if application is not None or logged_user is not None:
        if repository is None:
            installation_json = await github.get_installation_from_login(owner)
        else:
            installation_json = await github.get_installation_from_repository(
                owner, repository
            )

        if installation_json["suspended_at"]:
            raise fastapi.HTTPException(403)

        if application is not None:
            # Authenticated by token
            if application.github_account.id != installation_json["account"]["id"]:
                raise fastapi.HTTPException(status_code=403)
            github_client_auth = github.GitHubAppInstallationAuth(installation_json)
            actor = build_actor(auth_method=application)

        elif logged_user is not None:
            # Authenticated by cookie session
            github_client_auth = github.GitHubTokenAuth(logged_user.oauth_access_token)
            actor = build_actor(auth_method=logged_user)

        else:
            RuntimeError("logged_user and application must have been checked earlier")

        return _AuthenticationActor(
            installation=installation_json,
            github_client_auth=github_client_auth,
            actor=actor,
        )

    raise fastapi.HTTPException(403)


AuthenticatedActor = typing.Annotated[
    _AuthenticationActor, fastapi.Depends(_get_authenticated_actor)
]


def get_redis_key_for_repo_access_check(
    installation_account_login: github_types.GitHubLogin,
) -> str:
    return f"api/security/repositories-access/{installation_account_login}"


class _ApplicationAuthCache(typing.TypedDict):
    # GitHub's response to our repository request.
    # Will be set to `200` if the request was successfuly made,
    # otherwise it will contain the status code of the response's error.
    status_code: int
    # If status_code != 200, data contains the `exc.response.text`.
    # If status_code == 200, data contains the repository as github_types.GitHubRepository
    data: github_types.GitHubRepository | str


async def _application_actor_with_cache(
    owner: github_types.GitHubLogin,
    repository: github_types.GitHubRepositoryName,
    installation: github_types.GitHubInstallation,
    redis_links: redis.RedisLinks,
    client: github.AsyncGitHubInstallationClient,
) -> github_types.GitHubRepository:
    repo_access_redis_key = get_redis_key_for_repo_access_check(
        installation["account"]["login"]
    )
    raw_redis_value = await redis_links.cache.hget(
        repo_access_redis_key, f"{owner}/{repository}"
    )
    if raw_redis_value is not None:
        redis_value: _ApplicationAuthCache = json.loads(raw_redis_value)
        if redis_value["status_code"] != 200:
            # Means the installation's account doesn't have access to owner/repository
            # (set in the `except` case below)
            raise fastapi.HTTPException(
                status_code=404,
                detail=redis_value["data"],
            )

        return typing.cast(github_types.GitHubRepository, redis_value["data"])

    pipe = await redis_links.cache.pipeline()
    try:
        repo = typing.cast(
            github_types.GitHubRepository,
            await client.item(f"/repos/{owner}/{repository}"),
        )
        await pipe.hset(
            repo_access_redis_key,
            f"{owner}/{repository}",
            json.dumps({"status_code": 200, "data": repo}),
        )
    except (http.HTTPNotFound, http.HTTPForbidden, http.HTTPUnauthorized) as exc:
        await pipe.hset(
            repo_access_redis_key,
            f"{owner}/{repository}",
            json.dumps(
                {
                    "status_code": exc.status_code,
                    "data": exc.response.text,
                }
            ),
        )
        raise fastapi.HTTPException(status_code=404)
    finally:
        await pipe.expire(repo_access_redis_key, AUTH_CACHE_EXPIRE)
        await pipe.execute()

    return repo


async def get_repository_context(
    owner: RepositoryOwnerLogin,
    repository: RepositoryName,
    authenticated_actor: AuthenticatedActor,
    redis_links: redis.RedisLinks,
    application: LoggedApplication,
    logged_user: LoggedUser,
) -> abc.AsyncGenerator[context.Repository, None]:
    async with github.AsyncGitHubInstallationClient(
        authenticated_actor.github_client_auth,
    ) as client:
        if authenticated_actor.actor["type"] == "user":
            try:
                repo = typing.cast(
                    github_types.GitHubRepository,
                    await client.item(f"/repos/{owner}/{repository}"),
                )
            except (http.HTTPNotFound, http.HTTPForbidden, http.HTTPUnauthorized):
                raise fastapi.HTTPException(status_code=404)
        elif authenticated_actor.actor["type"] == "application":
            repo = await _application_actor_with_cache(
                owner, repository, authenticated_actor.installation, redis_links, client
            )
        else:
            raise RuntimeError(
                "Unknown actor type %s", authenticated_actor.actor["type"]
            )

        sub = await subscription.Subscription.get_subscription(
            redis_links.cache, repo["owner"]["id"]
        )
        if sub.has_feature(subscription.Features.PRIVATE_REPOSITORY):
            client.enable_extra_metrics()

        installation = context.Installation(
            authenticated_actor.installation, sub, client, redis_links
        )

        repository_ctxt = installation.get_repository_from_github_data(repo)

        if not repo["private"]:
            if logged_user is not None:
                try:
                    permission = await repository_ctxt.get_user_permission(
                        logged_user.to_github_account()
                    )
                except http.HTTPForbidden:
                    raise fastapi.HTTPException(status_code=403)

                if permission == github_types.GitHubRepositoryPermission.NONE:
                    raise fastapi.HTTPException(status_code=403)
            elif application is not None:
                if application.github_account_id != repo["owner"]["id"]:
                    raise fastapi.HTTPException(
                        status_code=403,
                        detail=f'{application.github_account_id} != {repo["owner"]["id"]}  - '
                        f'{application.github_account.login} != {repo["owner"]["login"]}',
                    )
            else:
                raise RuntimeError("get_http_auth should have raised 403")

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


async def check_subscription_feature_queue_pause(repository_ctxt: Repository) -> None:
    if not repository_ctxt.installation.subscription.has_feature(
        subscription.Features.QUEUE_PAUSE
    ):
        raise fastapi.HTTPException(
            status_code=402,
            detail="⚠ The subscription needs to be upgraded to enable the `queue_pause` feature.",
        )


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

_QueueNameFromPathPartiallyValidated = typing.Annotated[
    qr_config.QueueName,
    fastapi.Path(description="Name of the queue"),
    functional_validators.AfterValidator(utils.CheckNullChar),
]


async def get_queue_rule_by_name_from_path(
    queue_rules: QueueRules, queue_name: _QueueNameFromPathPartiallyValidated
) -> qr_config.QueueRule:
    try:
        return queue_rules[queue_name]
    except KeyError:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"The queue `{queue_name}` does not exist.",
        )


QueueRuleByNameFromPath = typing.Annotated[
    qr_config.QueueRule, fastapi.Depends(get_queue_rule_by_name_from_path)
]


async def get_queue_rule_name(
    queue_rule: QueueRuleByNameFromPath,
) -> qr_config.QueueName:
    return queue_rule.name


QueueNameFromPath = typing.Annotated[
    qr_config.QueueName, fastapi.Depends(get_queue_rule_name)
]


BranchFromPath = typing.Annotated[
    github_types.GitHubRefType,
    fastapi.Path(description="The name of the branch"),
    functional_validators.AfterValidator(utils.CheckNullChar),
]

OptionalBranchFromQuery = typing.Annotated[
    github_types.GitHubRefType | None,
    fastapi.Query(description="The name of the branch"),
    functional_validators.AfterValidator(utils.CheckNullChar),
]


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


_PartitionName = typing.Annotated[
    partr_config.PartitionRuleName,
    fastapi.Path(title="PartitionName", description="The name of the partition"),
    functional_validators.AfterValidator(utils.CheckNullChar),
]


async def get_validated_partition_name(
    partition_rules: PartitionRules, partition_name: _PartitionName
) -> partr_config.PartitionRuleName:
    if (
        partition_name != partr_config.DEFAULT_PARTITION_NAME
        and partition_name not in partition_rules
    ):
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"The partition `{partition_name}` does not exist.",
        )
    return partition_name


PartitionNameFromPath = typing.Annotated[
    partr_config.PartitionRuleName, fastapi.Depends(get_validated_partition_name)
]
