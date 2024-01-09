from __future__ import annotations

import base64
from collections import abc
import contextlib
import dataclasses
import datetime
import functools
import json
import random
import re
import typing
from urllib import parse

import daiquiri
from ddtrace import tracer
import first
from graphql_utils import multi
import msgpack

from mergify_engine import cache
from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import database
from mergify_engine import date
from mergify_engine import dependabot_helpers
from mergify_engine import dependabot_types
from mergify_engine import exceptions
from mergify_engine import flaky_check
from mergify_engine import github_graphql_types
from mergify_engine import github_types
from mergify_engine import pull_request_getter
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import subscription as subscription_mod
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.clients import github_app
from mergify_engine.clients import http
from mergify_engine.github_in_postgres import utils as ghinpg_utils
from mergify_engine.models.github import commit_status as status_model
from mergify_engine.models.github import pull_request_commit as prcommit_model
from mergify_engine.models.github import pull_request_file as prfile_model
from mergify_engine.rules.config import mergify as mergify_conf


if typing.TYPE_CHECKING:
    import logging

    from mergify_engine.models import github as gh_models

SUMMARY_SHA_EXPIRATION = 60 * 60 * 24 * 31 * 1  # 1 Month
WARNED_ABOUT_SHA_COLLISION_EXPIRATION = 60 * 60 * 24 * 7  # 7 days


class MergifyConfigFile(github_types.GitHubContentFile):
    decoded_content: str


@dataclasses.dataclass
class ConfigurationFileAlreadyLoadedError(Exception):
    mergify_config: mergify_conf.MergifyConfig | mergify_conf.InvalidRulesError

    def reraise_configuration_error(self) -> None:
        if isinstance(self.mergify_config, mergify_conf.InvalidRulesError):
            raise self.mergify_config


def content_file_to_config_file(
    content: github_types.GitHubContentFile,
) -> MergifyConfigFile:
    return MergifyConfigFile(
        type=content["type"],
        content=content["content"],
        path=content["path"],
        sha=content["sha"],
        encoding=content["encoding"],
        decoded_content=base64.b64decode(
            bytearray(content["content"], "utf-8"),
        ).decode(),
    )


DEFAULT_CONFIG_FILE = MergifyConfigFile(
    decoded_content="",
    type="file",
    content="<default>",
    sha=github_types.SHAType("<default>"),
    path=github_types.GitHubFilePath("<default>"),
    encoding="base64",
)


class PayloadEventSourceType(typing.TypedDict):
    event_type: github_types.GitHubEventType
    data: github_types.GitHubEvent
    timestamp: str
    initial_score: float


@dataclasses.dataclass
class InstallationCaches:
    team_members: cache.Cache[
        github_types.GitHubTeamSlug,
        list[github_types.GitHubLogin],
    ] = dataclasses.field(default_factory=cache.Cache)


@dataclasses.dataclass
class Installation:
    installation: github_types.GitHubInstallation
    subscription: subscription_mod.Subscription = dataclasses.field(repr=False)
    client: github.AsyncGitHubInstallationClient = dataclasses.field(repr=False)
    redis: redis_utils.RedisLinks = dataclasses.field(repr=False)

    repositories: dict[
        github_types.GitHubRepositoryName,
        Repository,
    ] = dataclasses.field(default_factory=dict, repr=False)
    _caches: InstallationCaches = dataclasses.field(
        default_factory=InstallationCaches,
        repr=False,
    )

    @property
    def owner_id(self) -> github_types.GitHubAccountIdType:
        return self.installation["account"]["id"]

    @property
    def owner_login(self) -> github_types.GitHubLogin:
        return self.installation["account"]["login"]

    USER_ID_MAPPING_CACHE_KEY = "user-id-mapping"

    async def get_user(
        self,
        login: github_types.GitHubLogin,
    ) -> github_types.GitHubAccount:
        data = await self.redis.cache.hget(self.USER_ID_MAPPING_CACHE_KEY, login)
        if data is not None:
            return typing.cast(github_types.GitHubAccount, json.loads(data))

        user = typing.cast(
            github_types.GitHubAccount,
            await self.client.item(f"/users/{login}"),
        )
        await self.redis.cache.hset(
            self.USER_ID_MAPPING_CACHE_KEY,
            login,
            json.dumps(user),
        )
        return user

    async def get_pull_request_context(
        self,
        repo_id: github_types.GitHubRepositoryIdType,
        pull_number: github_types.GitHubPullRequestNumber,
        force_new: bool = False,
    ) -> Context:
        for repository in self.repositories.values():
            if repository.repo["id"] == repo_id:
                return await repository.get_pull_request_context(
                    pull_number,
                    force_new=force_new,
                )

        pull = await pull_request_getter.get_pull_request(
            self.client,
            pull_number,
            repo_id,
            force_new=force_new,
        )
        repository = self.get_repository_from_github_data(pull["base"]["repo"])
        return await repository.get_pull_request_context(
            pull_number,
            pull,
            force_new=force_new,
        )

    def get_repository_from_github_data(
        self,
        repo: github_types.GitHubRepository | gh_models.GitHubRepositoryDict,
    ) -> Repository:
        if repo["name"] not in self.repositories:
            repository = Repository(self, repo)
            self.repositories[repo["name"]] = repository
        return self.repositories[repo["name"]]

    async def get_repository_by_name(
        self,
        name: github_types.GitHubRepositoryName,
    ) -> Repository:
        if name in self.repositories:
            return self.repositories[name]

        # Circular import
        from mergify_engine.models import github as gh_models

        async with database.create_session() as session:
            db_repo = await gh_models.GitHubRepository.get_by_name(
                session,
                self.owner_id,
                name,
            )

        repo_data: github_types.GitHubRepository | gh_models.GitHubRepositoryDict
        if db_repo is not None and db_repo.is_complete():
            repo_data = db_repo.as_github_dict()
        else:
            repo_data = await self.client.item(f"/repos/{self.owner_login}/{name}")
            await self._save_repository_to_database(repo_data)

        return self.get_repository_from_github_data(repo_data)

    async def _save_repository_to_database(
        self,
        repo_data: github_types.GitHubRepository | gh_models.GitHubRepositoryDict,
    ) -> None:
        # Circular import
        from mergify_engine.models import github as gh_models

        async for attempt in database.tenacity_retry_on_pk_integrity_error(
            (gh_models.GitHubRepository, gh_models.GitHubAccount),
        ):
            with attempt:
                async with database.create_session() as session:
                    db_repo = await gh_models.GitHubRepository.get_or_create(
                        session,
                        repo_data,
                    )
                    session.add(db_repo)
                    await session.commit()

    async def get_repository_by_id(
        self,
        _id: github_types.GitHubRepositoryIdType,
    ) -> Repository:
        for repository in self.repositories.values():
            if repository.repo["id"] == _id:
                return repository
        repo_data: github_types.GitHubRepository = await self.client.item(
            f"/repositories/{_id}",
        )
        return self.get_repository_from_github_data(repo_data)

    TEAM_MEMBERS_CACHE_KEY_PREFIX = "team_members"
    TEAM_MEMBERS_CACHE_KEY_DELIMITER = "/"
    TEAM_MEMBERS_EXPIRATION = 3600  # 1 hour

    @classmethod
    def _team_members_cache_key_for_repo(
        cls,
        owner_id: github_types.GitHubAccountIdType,
    ) -> str:
        return (
            f"{cls.TEAM_MEMBERS_CACHE_KEY_PREFIX}"
            f"{cls.TEAM_MEMBERS_CACHE_KEY_DELIMITER}{owner_id}"
        )

    @classmethod
    async def clear_team_members_cache_for_team(
        cls,
        redis: redis_utils.RedisTeamMembersCache,
        owner: github_types.GitHubAccount,
        team_slug: github_types.GitHubTeamSlug,
    ) -> None:
        await redis.hdel(
            cls._team_members_cache_key_for_repo(owner["id"]),
            team_slug,
        )

    @classmethod
    async def clear_team_members_cache_for_org(
        cls,
        redis: redis_utils.RedisTeamMembersCache,
        user: github_types.GitHubAccount,
    ) -> None:
        await redis.delete(cls._team_members_cache_key_for_repo(user["id"]))

    async def get_team_members(
        self,
        team_slug: github_types.GitHubTeamSlug,
    ) -> list[github_types.GitHubLogin]:
        members = self._caches.team_members.get(team_slug)
        if members is cache.Unset:
            key = self._team_members_cache_key_for_repo(self.owner_id)
            members_raw = await self.redis.team_members_cache.hget(key, team_slug)
            if members_raw is None:
                members = [
                    github_types.GitHubLogin(member["login"])
                    async for member in self.client.items(
                        f"/orgs/{self.owner_login}/teams/{team_slug}/members",
                        resource_name="team members",
                        page_limit=20,
                    )
                ]
                pipe = await self.redis.team_members_cache.pipeline()
                await pipe.hset(key, team_slug, msgpack.packb(members))
                await pipe.expire(key, self.TEAM_MEMBERS_EXPIRATION)
                await pipe.execute()
            else:
                members = typing.cast(
                    list[github_types.GitHubLogin],
                    msgpack.unpackb(members_raw),
                )
            self._caches.team_members.set(team_slug, members)
        return members


@dataclasses.dataclass
class RepositoryCaches:
    mergify_config_file: cache.SingleCache[
        MergifyConfigFile | None
    ] = dataclasses.field(default_factory=cache.SingleCache)
    mergify_config: cache.SingleCache[
        mergify_conf.MergifyConfig | mergify_conf.InvalidRulesError
    ] = dataclasses.field(default_factory=cache.SingleCache)
    branches: cache.Cache[
        github_types.GitHubRefType,
        github_types.GitHubBranch,
    ] = dataclasses.field(default_factory=cache.Cache)
    labels: cache.SingleCache[list[github_types.GitHubLabel]] = dataclasses.field(
        default_factory=cache.SingleCache,
    )
    branch_protections: cache.Cache[
        github_types.GitHubRefType,
        github_types.GitHubBranchProtection | None,
    ] = dataclasses.field(default_factory=cache.Cache)
    commits: cache.Cache[
        github_types.GitHubRefType,
        list[github_types.GitHubBranchCommit],
    ] = dataclasses.field(default_factory=cache.Cache)
    user_permissions: cache.Cache[
        github_types.GitHubAccountIdType,
        github_types.GitHubRepositoryPermission,
    ] = dataclasses.field(default_factory=cache.Cache)
    team_has_read_permission: cache.Cache[
        github_types.GitHubTeamSlug,
        bool,
    ] = dataclasses.field(default_factory=cache.Cache)


class MergifyInstalled(typing.TypedDict):
    installed: bool
    error: str | None


@dataclasses.dataclass
class Repository:
    installation: Installation
    repo: github_types.GitHubRepository | gh_models.GitHubRepositoryDict
    pull_contexts: dict[
        github_types.GitHubPullRequestNumber,
        Context,
    ] = dataclasses.field(default_factory=dict, repr=False)

    _caches: RepositoryCaches = dataclasses.field(
        default_factory=RepositoryCaches,
        repr=False,
    )
    log: logging.LoggerAdapter[logging.Logger] = dataclasses.field(
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        self.log = daiquiri.getLogger(
            self.__class__.__qualname__,
            gh_owner=self.installation.owner_login,
            gh_repo=self.repo["name"],
            gh_private=self.repo["private"],
        )

    @property
    def base_url(self) -> str:
        """The URL prefix to make GitHub request."""
        return f"/repos/{self.installation.owner_login}/{self.repo['name']}"

    async def iter_mergify_config_files(
        self,
        ref: github_types.SHAType | None = None,
        preferred_filename: github_types.GitHubFilePath | None = None,
    ) -> abc.AsyncIterator[MergifyConfigFile]:
        """Get the Mergify configuration file content.

        :return: The filename and its content.
        """

        params = {}
        if ref:
            params["ref"] = str(ref)
        else:
            params["ref"] = utils.extract_default_branch(self.repo)

        filenames = constants.MERGIFY_CONFIG_FILENAMES.copy()
        if preferred_filename:
            filenames.remove(preferred_filename)
            filenames.insert(0, preferred_filename)

        for filename in filenames:
            try:
                content = typing.cast(
                    github_types.GitHubContentFile,
                    await self.installation.client.item(
                        f"{self.base_url}/contents/{filename}",
                        params=params,
                    ),
                )
            except http.HTTPNotFoundError:
                continue
            except http.HTTPForbiddenError as e:
                codes = [e["code"] for e in e.response.json().get("errors", [])]
                if "too_large" in codes:
                    self.log.warning(
                        "configuration file too big, skipping it.",
                        config_filename=filename,
                    )
                    continue
                raise

            # More than 1M, see https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#get-contents
            if content["encoding"] == "none":
                self.log.warning(
                    "configuration file too big, skipping it.",
                    config_filename=filename,
                )
                continue

            # Mypy thinks it's unreachable, because the literal don't have more value
            # But GitHub API may have more we are not aware of as it's not well documented
            elif content["encoding"] != "base64":
                self.log.warning(  # type: ignore[unreachable]
                    "configuration has unhandled encoding, skipping it.",
                    config_filename=filename,
                    encoding=content["encoding"],
                )
                continue

            yield content_file_to_config_file(content)

    def clear_caches(self) -> None:
        self._caches = RepositoryCaches()

    async def load_mergify_config(
        self,
        config_file: MergifyConfigFile | None = None,
    ) -> None:
        # Circular import
        from mergify_engine.rules.config import mergify as mergify_conf

        mergify_config_or_exception = self._caches.mergify_config.get()
        if mergify_config_or_exception is not cache.Unset:
            raise ConfigurationFileAlreadyLoadedError(mergify_config_or_exception)

        if config_file is None:
            config_file = await self.get_mergify_config_file()
            if config_file is None:
                config_file = DEFAULT_CONFIG_FILE

        # BRANCH CONFIGURATION CHECKING
        try:
            mergify_config = await mergify_conf.get_mergify_config_from_file(
                self,
                config_file,
            )
        except mergify_conf.InvalidRulesError as e:
            self._caches.mergify_config.set(e)
            raise

        # Add global and mandatory rules
        builtin_mergify_config = await mergify_conf.get_mergify_builtin_config(
            self.installation.redis.cache,
        )
        mergify_config["pull_request_rules"].rules.extend(
            builtin_mergify_config["pull_request_rules"].rules,
        )
        self._caches.mergify_config.set(mergify_config)

    @contextlib.contextmanager
    def temporary_configuration(
        self,
        config: mergify_conf.MergifyConfig,
    ) -> abc.Generator[None, None, None]:
        real_config = self._caches.mergify_config.get()
        self._caches.mergify_config.set(config)
        try:
            yield
        finally:
            if real_config is cache.Unset:
                self._caches.mergify_config.delete()
            else:
                self._caches.mergify_config.set(real_config)

    @property
    def mergify_config(self) -> mergify_conf.MergifyConfig:
        mergify_config_or_exception = self._caches.mergify_config.get()
        if mergify_config_or_exception is cache.Unset:
            raise RuntimeError(
                "no mergify configuration has been loaded into the repository context",
            )
        if isinstance(mergify_config_or_exception, mergify_conf.InvalidRulesError):
            raise RuntimeError(
                "Trying to use the Mergify configuration after a loading failure",
            ) from mergify_config_or_exception
        return mergify_config_or_exception

    async def get_mergify_config_file(self) -> MergifyConfigFile | None:
        mergify_config_file = self._caches.mergify_config_file.get()
        if mergify_config_file is not cache.Unset:
            return mergify_config_file

        cached_config_file = await self.get_cached_config_file()
        if cached_config_file is not None:
            self._caches.mergify_config_file.set(cached_config_file)
            return cached_config_file

        config_file_cache_key = self.get_config_file_cache_key(self.repo["id"])
        pipeline = await self.installation.redis.cache.pipeline()

        async for config_file in self.iter_mergify_config_files():
            await pipeline.set(
                config_file_cache_key,
                json.dumps(
                    github_types.GitHubContentFile(
                        {
                            "type": config_file["type"],
                            "content": config_file["content"],
                            "path": config_file["path"],
                            "sha": config_file["sha"],
                            "encoding": config_file["encoding"],
                        },
                    ),
                ),
                ex=60 * 60 * 24 * 31,
            )
            self._caches.mergify_config_file.set(config_file)
            await pipeline.execute()
            return config_file

        self._caches.mergify_config_file.set(None)
        return None

    async def get_cached_config_file(self) -> MergifyConfigFile | None:
        config_file_raw = await self.installation.redis.cache.get(
            self.get_config_file_cache_key(self.repo["id"]),
        )

        if config_file_raw is None:
            return None

        content = typing.cast(
            github_types.GitHubContentFile,
            json.loads(config_file_raw),
        )
        # Backward compatibility add in 7.8.0
        content.setdefault("encoding", "base64")
        return content_file_to_config_file(content)

    async def get_branch(
        self,
        branch_name: github_types.GitHubRefType,
        bypass_cache: bool = False,
    ) -> github_types.GitHubBranch:
        branch = self._caches.branches.get(branch_name)
        if branch is cache.Unset or bypass_cache:
            escaped_branch_name = parse.quote(branch_name, safe="")
            branch = typing.cast(
                github_types.GitHubBranch,
                await self.installation.client.item(
                    f"{self.base_url}/branches/{escaped_branch_name}",
                ),
            )
            self._caches.branches.set(branch_name, branch)
        return branch

    async def delete_branch_if_exists(self, branch_name: str) -> bool:
        escaped_branch_name = parse.quote(branch_name, safe="")
        try:
            await self.installation.client.delete(
                f"{self.base_url}/git/refs/heads/{escaped_branch_name}",
            )
        except http.HTTPClientSideError as exc:
            if exc.status_code == 404 or (
                exc.status_code == 422 and "Reference does not exist" in exc.message
            ):
                return False
            raise

        return True

    async def get_pulls(
        self,
        state: typing.Literal["open", "closed", "all"] = "open",
        base: str | None = None,
        sort: typing.Literal[
            "created",
            "updated",
            "popularity",
            "long-running",
        ] = "created",
        sort_direction: typing.Literal["asc", "desc"] = "asc",
        per_page: int = 30,
        page: int = 1,
    ) -> list[github_types.GitHubPullRequest]:
        params: dict[str, typing.Any] = {
            "state": state,
            "sort": sort,
            "direction": sort_direction,
            "per_page": per_page,
            "page": page,
        }
        if base:
            params["base"] = base

        resp = await self.installation.client.get(
            f"{self.base_url}/pulls",
            params=params,
        )
        return [typing.cast(github_types.GitHubPullRequest, p) for p in resp.json()]

    async def get_pull_request_context(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        pull: github_types.GitHubPullRequest | None = None,
        force_new: bool = False,
    ) -> Context:
        if force_new or pull_number not in self.pull_contexts:
            if pull is None:
                pull = await pull_request_getter.get_pull_request(
                    self.installation.client,
                    pull_number,
                    self.repo["id"],
                    repo_owner=self.repo["owner"]["login"],
                    force_new=force_new,
                )
            elif pull["number"] != pull_number:
                raise RuntimeError(
                    'get_pull_request_context() needs pull["number"] == pull_number',
                )
            self.pull_contexts[pull_number] = Context(self, pull)

        return self.pull_contexts[pull_number]

    PULL_REQUEST_TITLE_CACHE_KEY_PREFIX = "pull_request_title"
    PULL_REQUEST_TITLE_CACHE_KEY_DELIMITER = "/"
    PULL_REQUEST_TITLE_EXPIRATION = datetime.timedelta(days=7)

    @classmethod
    def get_pull_request_title_cache_key(
        cls,
        repo_id: github_types.GitHubRepositoryIdType,
        pull_number: github_types.GitHubPullRequestNumber,
    ) -> str:
        return (
            f"{cls.PULL_REQUEST_TITLE_CACHE_KEY_PREFIX}"
            f"{cls.PULL_REQUEST_TITLE_CACHE_KEY_DELIMITER}{repo_id}"
            f"{cls.PULL_REQUEST_TITLE_CACHE_KEY_DELIMITER}{pull_number}"
        )

    async def get_pull_request_title(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
    ) -> str:
        """The returned data has good chance to be obsolete, the only intent is for
        caching the title for later reporting"""
        title_raw = await self.installation.redis.cache.get(
            self.get_pull_request_title_cache_key(self.repo["id"], pull_number),
        )
        if title_raw is None:
            ctxt = await self.get_pull_request_context(pull_number)
            title = ctxt.pull["title"]
            await self.cache_pull_request_title(
                self.installation.redis.cache,
                self.repo["id"],
                pull_number,
                title,
            )
        else:
            title = title_raw.decode()
        return title

    @classmethod
    async def cache_pull_request_title(
        cls,
        redis: redis_utils.RedisCache,
        repo_id: github_types.GitHubRepositoryIdType,
        pull_number: github_types.GitHubPullRequestNumber,
        title: str,
    ) -> None:
        await redis.set(
            cls.get_pull_request_title_cache_key(repo_id, pull_number),
            title,
            ex=cls.PULL_REQUEST_TITLE_EXPIRATION,
        )

    CONFIG_FILE_CACHE_KEY_PREFIX = "config_file"
    CONFIG_FILE_CACHE_KEY_DELIMITER = "/"

    @classmethod
    def get_config_file_cache_key(
        cls,
        repo_id: github_types.GitHubRepositoryIdType,
    ) -> str:
        return (
            f"{cls.CONFIG_FILE_CACHE_KEY_PREFIX}"
            f"{cls.CONFIG_FILE_CACHE_KEY_DELIMITER}{repo_id}"
        )

    @classmethod
    async def clear_config_file_cache(
        cls,
        redis: redis_utils.RedisCache,
        repo_id: github_types.GitHubRepositoryIdType,
    ) -> None:
        cache_key = cls.get_config_file_cache_key(repo_id)
        await redis.delete(cache_key)

    INSTALLATION_CACHE_EXPIRATION = datetime.timedelta(days=7)

    @classmethod
    def get_mergify_installation_cache_key(
        cls,
        repo_fullname: str,
    ) -> str:
        return f"mergify-installation/{repo_fullname}"

    async def is_mergify_installed(self) -> MergifyInstalled:
        """
        Returns True if Mergify is installed on the repository. Otherwise
        returns the error, as a string, from the http request
        """
        cache_key = self.get_mergify_installation_cache_key(self.repo["full_name"])
        cache_value = await self.installation.redis.cache.get(cache_key)
        if cache_value is not None:
            return typing.cast(MergifyInstalled, json.loads(cache_value))

        async with github.AsyncGitHubClient(
            auth=github_app.GitHubBearerAuth(),
        ) as client:
            try:
                await client.get(
                    f"/repos/{self.repo['owner']['login']}/{self.repo['name']}/installation",
                )
            except http.HTTPNotFoundError as e:
                ret = MergifyInstalled(
                    installed=False,
                    error=str(e),
                )
                await self.installation.redis.cache.set(
                    cache_key,
                    json.dumps(ret),
                    ex=self.INSTALLATION_CACHE_EXPIRATION,
                )
                return ret

        ret = MergifyInstalled(installed=True, error=None)
        await self.installation.redis.cache.set(
            cache_key,
            json.dumps(ret),
            ex=self.INSTALLATION_CACHE_EXPIRATION,
        )
        return ret

    USERS_PERMISSION_CACHE_KEY_PREFIX = "users_permission"
    USERS_PERMISSION_CACHE_KEY_DELIMITER = "/"
    USERS_PERMISSION_EXPIRATION = 3600  # 1 hour

    @classmethod
    def _users_permission_cache_key_for_repo(
        cls,
        owner_id: github_types.GitHubAccountIdType,
        repo_id: github_types.GitHubRepositoryIdType,
    ) -> str:
        return (
            f"{cls.USERS_PERMISSION_CACHE_KEY_PREFIX}"
            f"{cls.USERS_PERMISSION_CACHE_KEY_DELIMITER}{owner_id}"
            f"{cls.USERS_PERMISSION_CACHE_KEY_DELIMITER}{repo_id}"
        )

    @property
    def _users_permission_cache_key(self) -> str:
        return self._users_permission_cache_key_for_repo(
            self.installation.owner_id,
            self.repo["id"],
        )

    @classmethod
    async def clear_user_permission_cache_for_user(
        cls,
        redis: redis_utils.RedisUserPermissionsCache,
        owner: github_types.GitHubAccount,
        repo: github_types.GitHubRepository,
        user: github_types.GitHubAccount,
    ) -> None:
        await redis.hdel(
            cls._users_permission_cache_key_for_repo(owner["id"], repo["id"]),
            str(user["id"]),
        )

    @classmethod
    async def clear_user_permission_cache_for_repo(
        cls,
        redis: redis_utils.RedisUserPermissionsCache,
        owner: github_types.GitHubAccount,
        repo: github_types.GitHubRepository,
    ) -> None:
        await redis.delete(
            cls._users_permission_cache_key_for_repo(owner["id"], repo["id"]),
        )

    @classmethod
    async def clear_user_permission_cache_for_org(
        cls,
        redis: redis_utils.RedisUserPermissionsCache,
        user: github_types.GitHubAccount,
    ) -> None:
        pipeline = await redis.pipeline()
        async for key in redis.scan_iter(
            f"{cls.USERS_PERMISSION_CACHE_KEY_PREFIX}{cls.USERS_PERMISSION_CACHE_KEY_DELIMITER}{user['id']}{cls.USERS_PERMISSION_CACHE_KEY_DELIMITER}*",
            count=10000,
        ):
            await pipeline.delete(key)
        await pipeline.execute()

    async def get_user_permission(
        self,
        user: github_types.GitHubAccount,
    ) -> github_types.GitHubRepositoryPermission:
        permission = self._caches.user_permissions.get(user["id"])
        if permission is cache.Unset:
            key = self._users_permission_cache_key
            cached_permission_raw = (
                await self.installation.redis.user_permissions_cache.hget(
                    key,
                    str(user["id"]),
                )
            )
            if cached_permission_raw is None:
                permission = await self._get_user_permission_from_github(user)
                await self._set_permission_cache(user, permission)
            else:
                permission = github_types.GitHubRepositoryPermission(
                    cached_permission_raw.decode(),
                )
            self._caches.user_permissions.set(user["id"], permission)
        return permission

    async def _get_user_permission_from_github(
        self,
        user: github_types.GitHubAccount,
    ) -> github_types.GitHubRepositoryPermission:
        permission_response = await self.installation.client.item(
            f"{self.base_url}/collaborators/{user['login']}/permission",
        )
        permission_str = permission_response["permission"]

        try:
            return github_types.GitHubRepositoryPermission(permission_str)
        except ValueError:
            self.log.error(
                "Received unknown '%s' permission from GitHub. "
                "Keeps processing with none permission.",
                permission_str,
            )
            return github_types.GitHubRepositoryPermission.default()

    async def _set_permission_cache(
        self,
        user: github_types.GitHubAccount,
        permission: github_types.GitHubRepositoryPermission,
    ) -> None:
        pipe = await self.installation.redis.user_permissions_cache.pipeline()
        await pipe.hset(
            self._users_permission_cache_key,
            str(user["id"]),
            permission.value,
        )
        await pipe.expire(
            self._users_permission_cache_key,
            self.USERS_PERMISSION_EXPIRATION,
        )
        await pipe.execute()

    async def has_write_permission(self, user: github_types.GitHubAccount) -> bool:
        permission = await self.get_user_permission(user)
        return permission in github_types.GitHubRepositoryPermission.permissions_above(
            github_types.GitHubRepositoryPermission.WRITE,
        )

    TEAMS_PERMISSION_CACHE_KEY_PREFIX = "teams_permission"
    TEAMS_PERMISSION_CACHE_KEY_DELIMITER = "/"
    TEAMS_PERMISSION_EXPIRATION = 3600  # 1 hour

    @classmethod
    def _teams_permission_cache_key_for_repo(
        cls,
        owner_id: github_types.GitHubAccountIdType,
        repo_id: github_types.GitHubRepositoryIdType,
    ) -> str:
        return (
            f"{cls.TEAMS_PERMISSION_CACHE_KEY_PREFIX}"
            f"{cls.TEAMS_PERMISSION_CACHE_KEY_DELIMITER}{owner_id}"
            f"{cls.TEAMS_PERMISSION_CACHE_KEY_DELIMITER}{repo_id}"
        )

    @property
    def _teams_permission_cache_key(self) -> str:
        return self._teams_permission_cache_key_for_repo(
            self.installation.owner_id,
            self.repo["id"],
        )

    @classmethod
    async def clear_team_permission_cache_for_team(
        cls,
        redis: redis_utils.RedisTeamPermissionsCache,
        owner: github_types.GitHubAccount,
        team: github_types.GitHubTeamSlug,
    ) -> None:
        pipeline = await redis.pipeline()
        async for key in redis.scan_iter(
            f"{cls.TEAMS_PERMISSION_CACHE_KEY_PREFIX}{cls.TEAMS_PERMISSION_CACHE_KEY_DELIMITER}{owner['id']}{cls.TEAMS_PERMISSION_CACHE_KEY_DELIMITER}*",
            count=10000,
        ):
            await redis.hdel(key, team)
        await pipeline.execute()

    @classmethod
    async def clear_team_permission_cache_for_repo(
        cls,
        redis: redis_utils.RedisTeamPermissionsCache,
        owner: github_types.GitHubAccount,
        repo: github_types.GitHubRepository,
    ) -> None:
        await redis.delete(
            cls._teams_permission_cache_key_for_repo(owner["id"], repo["id"]),
        )

    @classmethod
    async def clear_team_permission_cache_for_org(
        cls,
        redis: redis_utils.RedisTeamPermissionsCache,
        org: github_types.GitHubAccount,
    ) -> None:
        pipeline = await redis.pipeline()
        async for key in redis.scan_iter(
            f"{cls.TEAMS_PERMISSION_CACHE_KEY_PREFIX}{cls.TEAMS_PERMISSION_CACHE_KEY_DELIMITER}{org['id']}{cls.TEAMS_PERMISSION_CACHE_KEY_DELIMITER}*",
            count=10000,
        ):
            await pipeline.delete(key)
        await pipeline.execute()

    async def team_has_read_permission(self, team: github_types.GitHubTeamSlug) -> bool:
        read_permission = self._caches.team_has_read_permission.get(team)
        if read_permission is cache.Unset:
            key = self._teams_permission_cache_key
            read_permission_raw = (
                await self.installation.redis.team_permissions_cache.hget(key, team)
            )
            if read_permission_raw is None:
                try:
                    # note(sileht) read permissions are not part of the permissions
                    # list as the api endpoint returns 404 if permission read is missing
                    # so no need to check permission
                    await self.installation.client.get(
                        f"/orgs/{self.installation.owner_login}/teams/{team}/repos/{self.installation.owner_login}/{self.repo['name']}",
                    )
                    read_permission = True
                except http.HTTPNotFoundError:
                    read_permission = False
                pipe = await self.installation.redis.team_permissions_cache.pipeline()
                await pipe.hset(key, team, str(int(read_permission)))
                await pipe.expire(key, self.TEAMS_PERMISSION_EXPIRATION)
                await pipe.execute()
            else:
                read_permission = bool(int(read_permission_raw))
            self._caches.team_has_read_permission.set(team, read_permission)
        return read_permission

    async def _get_branch_protection_from_branch(
        self,
        branch_name: github_types.GitHubRefType,
    ) -> github_types.GitHubBranchProtection | None:
        try:
            branch = await self.get_branch(branch_name)
        except http.HTTPNotFoundError:
            return None

        if branch["protection"]["enabled"]:
            return github_types.GitHubBranchProtection(
                {
                    "required_status_checks": branch["protection"][
                        "required_status_checks"
                    ],
                },
            )
        return None

    async def get_graphql_allowed_branch_protection_rules_fields(self) -> list[str]:
        # NOTE(Greesb): If this is one day used outside of the tests,
        # the request's response should be cached.
        query_fields = """
        query {
            __type(name: "BranchProtectionRule") {
                name
                kind
                description
                fields {
                    name
                }
            }
        }
        """
        response = await self.installation.client.graphql_post(query_fields)
        return [f["name"] for f in response["data"]["__type"]["fields"]]

    async def get_all_branch_protection_rules(
        self,
        pattern_branch_filter: str | None = None,
    ) -> list[github_graphql_types.GraphqlBranchProtectionRule]:
        field_names = await self.get_graphql_allowed_branch_protection_rules_fields()
        # Those fields may be absent in some GHES versions
        maybe_missing_fields = [
            "blocksCreations",  # GHES 3.5
            "lockBranch",  # GHES 3.8
            "requireLastPushApproval",  # GHES 3.8
            "requiredDeploymentEnvironments",  # GHES 3.9
            "requiresDeployments",  # GHES 3.9
        ]
        maybe_missing_fields_query = "\n".join(
            [f for f in maybe_missing_fields if f in field_names],
        )

        query = f"""
        query {{
            repository(owner: "{self.repo['owner']['login']}", name: "{self.repo['name']}") {{
                branchProtectionRules(first: 100) {{
                    nodes {{
                        allowsDeletions
                        allowsForcePushes
                        dismissesStaleReviews
                        isAdminEnforced
                        matchingRefs(first: 100) {{
                            nodes {{
                                name
                                prefix
                            }}
                        }}
                        pattern
                        requiredApprovingReviewCount
                        requiredStatusCheckContexts
                        requiresApprovingReviews
                        requiresCodeOwnerReviews
                        requiresCommitSignatures
                        requiresConversationResolution
                        requiresLinearHistory
                        requiresStatusChecks
                        requiresStrictStatusChecks
                        restrictsPushes
                        restrictsReviewDismissals
                        {maybe_missing_fields_query}
                    }}
                }}
            }}
        }}
        """
        response = await self.installation.client.graphql_post(query)
        nodes: list[github_graphql_types.GraphqlBranchProtectionRule] = []

        for node in response["data"]["repository"]["branchProtectionRules"]["nodes"]:
            if (
                pattern_branch_filter is not None
                and node["pattern"] != pattern_branch_filter
            ):
                continue

            node["matchingRefs"] = typing.cast(
                list[github_graphql_types.GraphqlBranchProtectionRuleMatchingRef],
                node["matchingRefs"]["nodes"],
            )
            nodes.append(node)

        return nodes

    async def get_branch_protection(
        self,
        branch_name: github_types.GitHubRefType,
    ) -> github_types.GitHubBranchProtection | None:
        branch_protection = self._caches.branch_protections.get(branch_name)
        if branch_protection is cache.Unset:
            escaped_branch_name = parse.quote(branch_name, safe="")
            try:
                branch_protection = typing.cast(
                    github_types.GitHubBranchProtection,
                    await self.installation.client.item(
                        f"{self.base_url}/branches/{escaped_branch_name}/protection",
                        api_version="luke-cage",
                    ),
                )
            except http.HTTPNotFoundError:
                branch_protection = None
            except http.HTTPForbiddenError as e:
                if (
                    "or make this repository public to enable this feature."
                    in e.message
                ):
                    branch_protection = None
                elif "Resource not accessible by integration" in e.message:
                    branch_protection = await self._get_branch_protection_from_branch(
                        branch_name,
                    )
                else:
                    raise

            self._caches.branch_protections.set(branch_name, branch_protection)
        return branch_protection

    async def get_labels(self) -> list[github_types.GitHubLabel]:
        labels = self._caches.labels.get()
        if labels is cache.Unset:
            labels = [
                label
                async for label in typing.cast(
                    abc.AsyncIterator[github_types.GitHubLabel],
                    self.installation.client.items(
                        f"{self.base_url}/labels",
                        resource_name="labels",
                        page_limit=7,
                    ),
                )
            ]
            self._caches.labels.set(labels)
        return labels

    async def ensure_label_exists(self, label_name: str) -> None:
        labels = await self.get_labels()
        names = [label["name"].lower() for label in labels]
        if label_name.lower() not in names:
            color = f"{random.randrange(16 ** 6):06x}"  # nosec
            try:
                resp = await self.installation.client.post(
                    f"{self.base_url}/labels",
                    json={"name": label_name, "color": color},
                )
            except http.HTTPClientSideError as e:
                self.log.warning(
                    "fail to create label",
                    label=label_name,
                    status_code=e.status_code,
                    error_message=e.message,
                )
                return
            else:
                label = typing.cast(github_types.GitHubLabel, resp.json())
                cached_labels = self._caches.labels.get()
                if cached_labels is not cache.Unset:
                    cached_labels.append(label)

    async def get_commits_diff_count(
        self,
        base_ref: (github_types.GitHubBaseBranchLabel | github_types.SHAType),
        head_ref: (github_types.GitHubHeadBranchLabel | github_types.SHAType),
    ) -> int | None:
        try:
            data = typing.cast(
                github_types.GitHubCompareCommits,
                await self.installation.client.item(
                    f"{self.base_url}/compare/{parse.quote(base_ref, safe='')}...{parse.quote(head_ref, safe='')}",
                ),
            )
        except http.HTTPClientSideError as e:
            if e.status_code == 404 or (
                e.status_code == 422
                and "this diff is taking too long to generate." in e.message
            ):
                return None
            raise
        else:
            if data["status"] in ("ahead", "identical"):
                return 0
            if data["status"] in ("behind", "diverged"):
                return data["behind_by"]
            return None


@dataclasses.dataclass
class ContextCaches:
    review_threads: cache.SingleCache[
        list[github_graphql_types.CachedReviewThread],
    ] = dataclasses.field(default_factory=cache.SingleCache)
    consolidated_reviews: cache.SingleCache[
        tuple[
            list[github_types.GitHubReview],
            list[github_types.GitHubReview],
        ],
    ] = dataclasses.field(default_factory=cache.SingleCache)
    pull_check_runs: cache.SingleCache[
        list[github_types.CachedGitHubCheckRun]
    ] = dataclasses.field(default_factory=cache.SingleCache)
    pull_statuses: cache.SingleCache[
        list[github_types.GitHubStatus]
    ] = dataclasses.field(default_factory=cache.SingleCache)
    reviews: cache.SingleCache[list[github_types.GitHubReview]] = dataclasses.field(
        default_factory=cache.SingleCache,
    )
    is_behind: cache.SingleCache[bool] = dataclasses.field(
        default_factory=cache.SingleCache,
    )
    review_decision: cache.SingleCache[
        github_graphql_types.GitHubPullRequestReviewDecision
    ] = dataclasses.field(default_factory=cache.SingleCache)
    is_conflicting: cache.SingleCache[bool] = dataclasses.field(
        default_factory=cache.SingleCache,
    )
    files: cache.SingleCache[list[github_types.CachedGitHubFile]] = dataclasses.field(
        default_factory=cache.SingleCache,
    )
    commits: cache.SingleCache[
        list[github_types.CachedGitHubBranchCommit]
    ] = dataclasses.field(default_factory=cache.SingleCache)
    commits_behind_count: cache.SingleCache[int] = dataclasses.field(
        default_factory=cache.SingleCache,
    )


@dataclasses.dataclass
class Context:
    repository: Repository
    pull: github_types.GitHubPullRequest
    sources: list[PayloadEventSourceType] = dataclasses.field(default_factory=list)
    configuration_changed: bool = False
    github_has_pending_background_jobs: bool = dataclasses.field(
        init=False,
        default=False,
    )
    log: logging.LoggerAdapter[logging.Logger] = dataclasses.field(
        init=False,
        repr=False,
    )
    flaky_checks_to_rerun: list[flaky_check.CheckToRerunResult] = dataclasses.field(
        default_factory=list,
    )

    _caches: ContextCaches = dataclasses.field(
        default_factory=ContextCaches,
        repr=False,
    )

    def __post_init__(self) -> None:
        self.log = daiquiri.getLogger(
            self.__class__.__qualname__,
            gh_pull=self.pull["number"],
            gh_author=self.pull["user"]["login"]
            if self.pull["user"] is not None
            else "<unknown>",
            gh_owner=self.pull["base"]["user"]["login"]
            if "base" in self.pull
            else "<unknown>",
            gh_repo=(
                self.pull["base"]["repo"]["name"]
                if "base" in self.pull
                else "<unknown>"
            ),
            gh_private=(
                self.pull["base"]["repo"]["private"]
                if "base" in self.pull
                else "<unknown>"
            ),
            gh_branch=self.pull["base"]["ref"] if "base" in self.pull else "<unknown>",
            gh_pull_head_ref=self.pull["head"]["ref"]
            if "head" in self.pull
            else "<unknown>",
            gh_pull_head_owner=self.pull["head"]["user"]["login"]
            if "head" in self.pull and self.pull["head"]["user"] is not None
            else "<unknown>",
            gh_pull_base_sha=self.pull["base"]["sha"]
            if "base" in self.pull
            else "<unknown>",
            gh_pull_head_sha=self.pull["head"]["sha"]
            if "head" in self.pull
            else "<unknown>",
            gh_pull_locked=self.pull["locked"],
            gh_pull_merge_commit_sha=self.pull["merge_commit_sha"],
            gh_pull_url=self.pull.get("html_url", "<unknown-yet>"),
            gh_pull_mergeable=self.pull.get("mergeable", "none"),
            gh_pull_mergeable_state=(
                "merged"
                if self.pull.get("merged")
                else (self.pull.get("mergeable_state", "unknown") or "none")
            ),
        )

    @property
    def redis(self) -> redis_utils.RedisLinks:
        # TODO(sileht): remove me when context split if done
        return self.repository.installation.redis

    @property
    def subscription(self) -> subscription_mod.Subscription:
        # TODO(sileht): remove me when context split if done
        return self.repository.installation.subscription

    @property
    def client(self) -> github.AsyncGitHubInstallationClient:
        # TODO(sileht): remove me when context split if done
        return self.repository.installation.client

    @property
    def base_url(self) -> str:
        # TODO(sileht): remove me when context split if done
        return self.repository.base_url

    @property
    def repo_owner_login(self) -> github_types.GitHubLogin:
        return self.repository.repo["owner"]["login"]

    async def retrieve_unverified_commits(self) -> list[str]:
        return [
            commit.commit_message
            for commit in await self.commits
            if not commit.commit_verification_verified
        ]

    @functools.cached_property
    def _most_recent_event_datetime(self) -> datetime.datetime | None:
        timestamps = [
            date.fromisoformat(source["data"]["received_at"])
            for source in self.sources
            # NOTE(sileht): backward compat for refresh event without
            # "received_at" set
            if "received_at" in source["data"]
        ]
        if timestamps:
            return max(timestamps)
        return None

    async def retrieve_review_threads(
        self,
    ) -> list[github_graphql_types.CachedReviewThread]:
        review_threads = self._caches.review_threads.get()
        if review_threads is cache.Unset:
            query = """
                repository(owner: "{owner}", name: "{name}") {{
                    pullRequest(number: {number}) {{
                        reviewThreads(first: 100{after}) {{
                        edges {{
                            node {{
                            isResolved
                            comments(first: 1) {{
                                edges {{
                                    node {{
                                        body
                                    }}
                                }}
                            }}
                            }}
                        }}
                        }}
                    }}
                }}
            """
            responses = typing.cast(
                abc.AsyncIterable[
                    dict[str, github_graphql_types.GraphqlRepositoryForReviewThreads]
                ],
                multi.multi_query(
                    query,
                    iterable=(
                        {
                            "owner": self.repository.repo["owner"]["login"],
                            "name": self.repository.repo["name"],
                            "number": self.pull["number"],
                        },
                    ),
                    send_fn=self.client.graphql_post,
                ),
            )
            review_threads = []
            async for response in responses:
                for current_response in response.values():
                    for thread in current_response["pullRequest"]["reviewThreads"][
                        "edges"
                    ]:
                        review_threads.append(
                            github_graphql_types.CachedReviewThread(
                                {
                                    "isResolved": thread["node"]["isResolved"],
                                    "first_comment": thread["node"]["comments"][
                                        "edges"
                                    ][0]["node"]["body"],
                                },
                            ),
                        )
            self._caches.review_threads.set(review_threads)
        return review_threads

    async def retrieve_review_decision(
        self,
    ) -> github_graphql_types.GitHubPullRequestReviewDecision:
        review_decision = self._caches.review_decision.get()
        if review_decision is cache.Unset:
            query = f"""
{{
  repository(owner: "{self.repository.repo["owner"]["login"]}", name: "{self.repository.repo["name"]}") {{
    pullRequest(number: {self.pull["number"]}) {{
      reviewDecision
    }}
  }}
}}
"""
            response = await self.client.graphql_post(query)
            review_decision = typing.cast(
                github_graphql_types.GitHubPullRequestReviewDecision,
                response["data"]["repository"]["pullRequest"]["reviewDecision"],
            )
            self._caches.review_decision.set(review_decision)
        return review_decision

    async def set_summary_check(
        self,
        result: check_api.Result,
    ) -> github_types.CachedGitHubCheckRun:
        """Set the Mergify Summary check result."""

        previous_sha = await self.get_cached_last_summary_head_sha()
        # NOTE(sileht): we first commit in redis the future sha,
        # so engine.create_initial_summary() cannot creates a second SUMMARY
        # We don't delete the old redis_last_summary_pulls_key in case of the
        # API call fails, so no other pull request can takeover this sha
        await self._save_cached_last_summary_head_sha(self.pull["head"]["sha"])

        try:
            return await check_api.set_check_run(
                self,
                constants.SUMMARY_NAME,
                result,
                external_id=str(self.pull["number"]),
                skip_cache=self._caches.pull_check_runs.get() is cache.Unset,
            )
        except Exception:
            if previous_sha:
                # Restore previous sha in redis
                await self._save_cached_last_summary_head_sha(
                    previous_sha,
                    self.pull["head"]["sha"],
                )
            raise

    @staticmethod
    def redis_merged_by_mergify_key(
        owner_id: github_types.GitHubAccountIdType,
        repo_id: github_types.GitHubRepositoryIdType,
        merge_commit_sha: github_types.SHAType,
    ) -> str:
        return f"merged-by-mergify~{owner_id}~{repo_id}~{merge_commit_sha}"

    async def is_sha_merged_by_mergify(
        self,
        merge_commit_sha: github_types.SHAType,
    ) -> bool:
        stored_merge_commit_sha = await self.redis.queue.get(
            self.redis_merged_by_mergify_key(
                self.repository.installation.owner_id,
                self.repository.repo["id"],
                merge_commit_sha,
            ),
        )
        return stored_merge_commit_sha is not None

    @staticmethod
    def redis_last_summary_head_sha_key(pull: github_types.GitHubPullRequest) -> str:
        owner = pull["base"]["repo"]["owner"]["id"]
        repo = pull["base"]["repo"]["id"]
        pull_number = pull["number"]
        return f"summary-sha~{owner}~{repo}~{pull_number}"

    @staticmethod
    def redis_last_summary_pulls_key(
        owner_id: github_types.GitHubAccountIdType,
        repo_id: github_types.GitHubRepositoryIdType,
        sha: github_types.SHAType,
    ) -> str:
        return f"summary-pulls~{owner_id}~{repo_id}~{sha}"

    @staticmethod
    def redis_warned_about_sha_collision_key(
        pull: github_types.GitHubPullRequest,
    ) -> str:
        owner_id = pull["base"]["repo"]["owner"]["id"]
        repo_id = pull["base"]["repo"]["id"]
        sha = pull["head"]["sha"]
        return f"pr-sha-collision-warned~{owner_id}~{repo_id}~{pull['number']}~{sha}"

    async def set_warned_about_sha_collision(self, comment_url: str) -> None:
        await self.redis.cache.set(
            self.redis_warned_about_sha_collision_key(self.pull),
            comment_url,
            ex=WARNED_ABOUT_SHA_COLLISION_EXPIRATION,
        )

    async def get_warned_about_sha_collision(self) -> bool:
        return bool(
            await self.redis.cache.exists(
                self.redis_warned_about_sha_collision_key(self.pull),
            ),
        )

    @classmethod
    async def get_cached_last_summary_head_sha_from_pull(
        cls,
        redis_cache: redis_utils.RedisCache,
        pull: github_types.GitHubPullRequest,
    ) -> github_types.SHAType | None:
        raw = await redis_cache.get(cls.redis_last_summary_head_sha_key(pull))
        if raw is None:
            return None
        return github_types.SHAType(raw.decode())

    @classmethod
    async def summary_exists(
        cls,
        redis_cache: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
        repo_id: github_types.GitHubRepositoryIdType,
        pull: github_types.GitHubPullRequest,
    ) -> bool:
        sha_exists = bool(
            await redis_cache.exists(
                cls.redis_last_summary_pulls_key(
                    owner_id,
                    repo_id,
                    pull["head"]["sha"],
                ),
            ),
        )
        if sha_exists:
            return True

        sha = await cls.get_cached_last_summary_head_sha_from_pull(redis_cache, pull)
        return sha is not None and sha == pull["head"]["sha"]

    async def get_cached_last_summary_head_sha(
        self,
    ) -> github_types.SHAType | None:
        return await self.get_cached_last_summary_head_sha_from_pull(
            self.redis.cache,
            self.pull,
        )

    async def clear_cached_last_summary_head_sha(self) -> None:
        pipe = await self.redis.cache.pipeline()
        await pipe.delete(self.redis_last_summary_head_sha_key(self.pull))
        await pipe.delete(
            self.redis_last_summary_pulls_key(
                self.repository.installation.owner_id,
                self.repository.repo["id"],
                self.pull["head"]["sha"],
            ),
            str(self.pull["number"]),
        )
        await pipe.execute()

    async def _save_cached_last_summary_head_sha(
        self,
        sha: github_types.SHAType,
        old_sha: github_types.SHAType | None = None,
    ) -> None:
        # NOTE(sileht): We store it only for 1 month, if we lose it it's not a big deal, as it's just
        # to avoid race conditions when too many synchronize events occur in a short period of time
        pipe = await self.redis.cache.pipeline()
        await pipe.set(
            self.redis_last_summary_head_sha_key(self.pull),
            sha,
            ex=SUMMARY_SHA_EXPIRATION,
        )
        await pipe.set(
            self.redis_last_summary_pulls_key(
                self.repository.installation.owner_id,
                self.repository.repo["id"],
                sha,
            ),
            self.pull["number"],
            ex=SUMMARY_SHA_EXPIRATION,
        )
        if old_sha is not None:
            await pipe.delete(
                self.redis_last_summary_pulls_key(
                    self.repository.installation.owner_id,
                    self.repository.repo["id"],
                    old_sha,
                ),
            )
        await pipe.execute()

    async def consolidated_reviews(
        self,
    ) -> tuple[list[github_types.GitHubReview], list[github_types.GitHubReview]]:
        consolidated_reviews = self._caches.consolidated_reviews.get()
        if consolidated_reviews is cache.Unset:
            # Ignore reviews that are not from someone with admin/write permissions
            # And only keep the last review for each user.
            comments: dict[github_types.GitHubLogin, github_types.GitHubReview] = {}
            approvals: dict[github_types.GitHubLogin, github_types.GitHubReview] = {}
            valid_user_ids = {
                r["user"]["id"]
                for r in await self.reviews
                if (
                    r["user"] is not None
                    and (
                        r["user"]["type"] == "Bot"
                        or await self.repository.has_write_permission(r["user"])
                    )
                )
            }

            for review in await self.reviews:
                if not review["user"] or review["user"]["id"] not in valid_user_ids:
                    continue
                # Only keep latest review of an user
                if review["state"] == "COMMENTED":
                    comments[review["user"]["login"]] = review
                else:
                    approvals[review["user"]["login"]] = review

            consolidated_reviews = list(comments.values()), list(approvals.values())
            self._caches.consolidated_reviews.set(consolidated_reviews)
        return consolidated_reviews

    @property
    async def dependabot_attributes(
        self,
    ) -> list[dependabot_types.DependabotAttributes]:
        if self.pull["user"]["login"] != constants.DEPENDABOT_PULL_REQUEST_AUTHOR_LOGIN:
            return []
        commits = await self.commits
        return dependabot_helpers.get_dependabot_consolidated_data_from_commit_msg(
            self.log,
            commits[0].commit_message,
        )

    async def get_merge_queue_check_run_name(self) -> str:
        check = await self.get_merge_queue_check_run()
        if check is None:
            return constants.MERGE_QUEUE_SUMMARY_NAME
        return check["name"]

    async def get_merge_queue_check_run(
        self,
    ) -> github_types.CachedGitHubCheckRun | None:
        check = await self.get_engine_check_run(constants.MERGE_QUEUE_SUMMARY_NAME)
        if check is None:
            check = await self.get_engine_check_run(
                constants.MERGE_QUEUE_OLD_SUMMARY_NAME,
            )
        return check

    DEPENDS_ON = re.compile(
        r"^ *Depends-On: +(?:#|"
        + settings.GITHUB_URL
        + r"/(?P<owner>[^/]+)/(?P<repo>[^/]+)/pull/)(?P<pull>\d+) *$",
        re.MULTILINE | re.IGNORECASE,
    )

    CONFLICT_EXPIRATION = datetime.timedelta(days=30)

    @property
    def _conflict_cache_key(self) -> str:
        return f"conflict/{self.repository.repo['id']}/{self.pull['number']}"

    async def is_conflicting(self) -> bool:
        if self.closed:
            # NOTE(sileht): this mimic the GitHub behavior that doesn't
            # compute it anymore when the PR is closed.
            return False

        is_conflicting = self._caches.is_conflicting.get()
        if is_conflicting is cache.Unset:
            if self.pull["mergeable"] is None:
                # NOTE(sileht): we mark it, so at the end of the engine
                # processing we will refresh the PR later
                self.github_has_pending_background_jobs = True

                cached_is_conflicting: bytes | None = await self.redis.cache.get(
                    self._conflict_cache_key,
                )
                # NOTE(sileht): Here we fallback to the last known value or False
                if cached_is_conflicting is None:
                    is_conflicting = False
                else:
                    is_conflicting = bool(int(cached_is_conflicting))
            else:
                is_conflicting = self.pull["mergeable"] is False

            await self.redis.cache.set(
                self._conflict_cache_key,
                str(int(is_conflicting)),
                ex=self.CONFLICT_EXPIRATION,
            )
            self._caches.is_conflicting.set(is_conflicting)

        return is_conflicting

    @property
    def body(self) -> str:
        # NOTE(sileht): multiline regex on our side assume eol char is only LF,
        # not CR. So ensure we don't have CRLF in the body
        if self.pull["body"] is None:
            return ""
        return self.pull["body"].replace("\r\n", "\n")

    def get_depends_on(self) -> list[github_types.GitHubPullRequestNumber]:
        return sorted(
            {
                github_types.GitHubPullRequestNumber(int(pull))
                for owner, repo, pull in self.DEPENDS_ON.findall(self.body)
                if (owner == "" and repo == "")
                or (
                    owner == self.pull["base"]["user"]["login"]
                    and repo == self.pull["base"]["repo"]["name"]
                )
            },
        )

    # merge-after: YEAR-MONTH-DAY HOUR:MINUTES
    MERGE_AFTER = re.compile(
        r"^\s*Merge-After:\s*([^\n]*)$",
        re.MULTILINE | re.IGNORECASE,
    )

    def get_merge_after(self) -> datetime.datetime | None:
        find = self.MERGE_AFTER.search(self.body)
        if not find:
            return None

        try:
            return date.fromisoformat_with_zoneinfo(find.group(1))
        except date.InvalidDateError:
            return None

    async def update_cached_check_runs(
        self,
        check: github_types.CachedGitHubCheckRun,
    ) -> None:
        if self._caches.pull_check_runs.get() is cache.Unset:
            return

        pull_check_runs = [
            c for c in await self.pull_check_runs if c["name"] != check["name"]
        ]
        pull_check_runs.append(check)
        self._caches.pull_check_runs.set(pull_check_runs)

    @property
    def pull_labels_names(self) -> set[str]:
        return {label["name"].lower() for label in self.pull["labels"]}

    @property
    async def pull_check_runs(self) -> list[github_types.CachedGitHubCheckRun]:
        checks = self._caches.pull_check_runs.get()
        if checks is cache.Unset:
            checks = await check_api.get_checks_for_ref(self, self.pull["head"]["sha"])
            self._caches.pull_check_runs.set(checks)
        return checks

    @property
    async def pull_engine_check_runs(
        self,
    ) -> list[github_types.CachedGitHubCheckRun]:
        return [
            c
            for c in await self.pull_check_runs
            if c["app_id"] == settings.GITHUB_APP_ID
        ]

    async def get_engine_check_run(
        self,
        name: str,
    ) -> github_types.CachedGitHubCheckRun | None:
        return first.first(
            await self.pull_engine_check_runs,
            key=lambda c: c["name"] == name,
        )

    @property
    async def pull_statuses(self) -> list[github_types.GitHubStatus]:
        statuses = self._caches.pull_statuses.get()
        if statuses is cache.Unset:
            if await ghinpg_utils.can_repo_use_github_in_pg_data(
                repo_owner=self.repository.repo["owner"]["login"],
            ):
                async with database.create_session() as session:
                    statuses = await status_model.Status.get_pull_request_statuses(
                        session,
                        self.pull,
                        self.repository.repo["id"],
                    )
            else:
                statuses = [
                    s
                    async for s in typing.cast(
                        abc.AsyncIterable[github_types.GitHubStatus],
                        self.client.items(
                            f"{self.base_url}/commits/{self.pull['head']['sha']}/status",
                            list_items="statuses",
                            resource_name="statuses",
                            page_limit=10,
                        ),
                    )
                ]
            self._caches.pull_statuses.set(statuses)
        return statuses

    @property
    async def checks(
        self,
    ) -> dict[
        str,
        (github_types.GitHubCheckRunConclusion | github_types.GitHubStatusState),
    ]:
        # NOTE(sileht): check-runs are returned in reverse chronogical order,
        # so if it has ran twice we must keep only the more recent
        # statuses are good as GitHub already ensures the uniqueness of the name

        checks: dict[
            str,
            (github_types.GitHubCheckRunConclusion | github_types.GitHubStatusState),
        ] = {}

        # First put all branch protections checks as pending and then override with
        # the real status
        protection = await self.repository.get_branch_protection(
            self.pull["base"]["ref"],
        )
        if (
            protection
            and "required_status_checks" in protection
            and protection["required_status_checks"]
        ):
            checks.update(
                {
                    context: "pending"
                    for context in protection["required_status_checks"]["contexts"]
                },
            )

        pull_check_runs = await self.pull_check_runs

        self.flaky_checks_to_rerun = await flaky_check.get_checks_to_rerun(
            self.repository,
            pull_check_runs,
        )

        # NOTE(sileht): conclusion can be one of success, failure, neutral,
        # cancelled, timed_out, or action_required, and  None for "pending"
        checks.update(
            {
                c["name"]: c["conclusion"]
                for c in sorted(pull_check_runs, key=self._check_runs_sorter)
            },
        )
        # NOTE(sileht): state can be one of error, failure, pending,
        # or success.
        checks.update({s["context"]: s["state"] for s in await self.pull_statuses})

        # NOTE(Kontrolix): Makeup results to pending for checks that need reruns
        # or those that we don't know yet
        checks.update(
            {check["check_name"]: None for check in self.flaky_checks_to_rerun},
        )

        return checks

    @staticmethod
    def _check_runs_sorter(
        check_run: github_types.CachedGitHubCheckRun,
    ) -> datetime.datetime:
        if check_run["completed_at"] is None:
            return datetime.datetime.max.replace(tzinfo=date.UTC)
        return datetime.datetime.fromisoformat(check_run["completed_at"])

    @tracer.wrap("ensure_complete")
    async def ensure_complete(self) -> None:
        if not self._is_data_complete():
            self.pull = await pull_request_getter.get_pull_request(
                self.client,
                self.pull["number"],
                self.repository.repo["id"],
                repo_owner=self.repo_owner_login,
            )

    def _is_data_complete(self) -> bool:
        # NOTE(sileht): If pull request come from /pulls listing or check-runs sometimes,
        # they are incomplete, This ensure we have the complete view
        fields_to_control = (
            "state",
            "mergeable",
            "merge_commit_sha",
            "merged_by",
            "merged",
            "merged_at",
        )
        return all(field in self.pull for field in fields_to_control)

    async def update(
        self,
        wait_merged: bool = False,
        wait_merge_commit_sha: bool = False,
    ) -> None:
        # Don't use it, because consolidated data are not updated after that.
        # Only used by merge/queue action for posting an update report after rebase.
        self.pull = await self.client.item(
            f"{self.base_url}/pulls/{self.pull['number']}",
            extensions={
                "retry": lambda response: (
                    response.status_code == 200
                    and (
                        (wait_merged and not response.json()["merged"])
                        or (
                            wait_merge_commit_sha
                            and not response.json()["merge_commit_sha"]
                        )
                    )
                ),
            },
        )
        self._caches.pull_check_runs.delete()

    async def _get_heads_from_commit(self) -> set[github_types.SHAType]:
        shas: set[github_types.SHAType] = set()
        parents: set[github_types.SHAType] = set()
        for commit in await self.commits:
            shas.add(commit.sha)
            parents |= set(commit.parents)
        return {sha for sha in shas if sha not in parents}

    async def _get_external_parents(self) -> set[github_types.SHAType]:
        known_commits_sha = [commit.sha for commit in await self.commits]
        external_parents_sha = set()
        for commit in await self.commits:
            for parent_sha in commit.parents:
                if parent_sha not in known_commits_sha:
                    external_parents_sha.add(parent_sha)
        return external_parents_sha

    @property
    async def commits_behind_count(self) -> int:
        commits_behind_count = self._caches.commits_behind_count.get()
        if commits_behind_count is cache.Unset:
            if self.pull["merged"]:
                commits_behind_count = 0
            else:
                commits_diff_count = await self.repository.get_commits_diff_count(
                    self.pull["base"]["label"],
                    self.pull["head"]["sha"],
                )
                if commits_diff_count is None:
                    commits_behind_count = 1000000
                else:
                    commits_behind_count = commits_diff_count
            self._caches.commits_behind_count.set(commits_behind_count)
        return commits_behind_count

    async def has_linear_history(self) -> bool:
        return all(len(commit.parents) == 1 for commit in await self.commits)

    async def is_head_sha_outdated(self) -> bool:
        commit_heads = await self._get_heads_from_commit()
        return self.pull["head"]["sha"] not in commit_heads

    @property
    async def is_behind(self) -> bool:
        is_behind = self._caches.is_behind.get()
        if is_behind is cache.Unset:
            if self.pull["merged"]:
                is_behind = False
            else:
                # FIXME(sileht): check if we can leverage compare API here like
                # commits_behind_count by comparing branch label with head sha
                branch = await self.repository.get_branch(
                    self.pull["base"]["ref"],
                    bypass_cache=True,
                )
                commit_heads = await self._get_heads_from_commit()
                external_parents_sha = await self._get_external_parents()
                is_behind = branch["commit"]["sha"] not in external_parents_sha
                is_behind_testing = await self.commits_behind_count != 0
                is_behind_obsolete = self.pull["head"]["sha"] not in commit_heads
                if is_behind_testing != is_behind or is_behind_obsolete:
                    self.log.error(
                        "is_behind testing",
                        is_behind_obsolete=is_behind_obsolete,
                        is_behind_testing=is_behind_testing,
                        is_behind=is_behind,
                        behind_by=await self.commits_behind_count,
                        commit_heads=commit_heads,
                    )
            self._caches.is_behind.set(is_behind)
        return is_behind

    async def synchronized_by_user_at(self) -> datetime.datetime | None:
        for source in self.sources:
            if source["event_type"] == "pull_request":
                event = typing.cast(github_types.GitHubEventPullRequest, source["data"])
                if event["action"] == "synchronize":
                    mergify_bot = await github.GitHubAppInfo.get_bot(
                        self.repository.installation.redis.cache,
                    )
                    is_mergify = event["sender"]["id"] == mergify_bot[
                        "id"
                    ] or await self.redis.cache.get(
                        f"branch-update-{self.pull['head']['sha']}",
                    )
                    if not is_mergify:
                        return date.fromisoformat(event["received_at"])
        return None

    def has_been_synchronized(self) -> bool:
        for source in self.sources:
            if source["event_type"] == "pull_request":
                event = typing.cast(github_types.GitHubEventPullRequest, source["data"])
                if event["action"] == "synchronize":
                    return True
        return False

    def has_been_only_refreshed(self) -> bool:
        return all(source["event_type"] == "refresh" for source in self.sources)

    def has_been_opened(self) -> bool:
        for source in self.sources:
            if source["event_type"] == "pull_request":
                event = typing.cast(github_types.GitHubEventPullRequest, source["data"])
                if event["action"] == "opened":
                    return True
        return False

    def __str__(self) -> str:
        login = self.pull["base"]["user"]["login"]
        repo = self.pull["base"]["repo"]["name"]
        number = self.pull["number"]
        branch = self.pull["base"]["ref"]
        return f"{login}/{repo}/pull/{number}@{branch}"

    @property
    async def reviews(self) -> list[github_types.GitHubReview]:
        reviews = self._caches.reviews.get()
        if reviews is cache.Unset:
            reviews = [
                review
                async for review in typing.cast(
                    abc.AsyncIterable[github_types.GitHubReview],
                    self.client.items(
                        f"{self.base_url}/pulls/{self.pull['number']}/reviews",
                        resource_name="reviews",
                        page_limit=5,
                    ),
                )
                # NOTE(sileht): We ignore any review done after the last event
                # we received. It's safe because we will received another
                # review submitted event soon for the review we filter out
                # This allows to have a coherent view between review data
                # retrived in this API and the review data in pull request
                # requested_reviewers and requested_teams attributes
                if review is not None
                and (
                    self._most_recent_event_datetime is None
                    or date.fromisoformat(review["submitted_at"])
                    <= self._most_recent_event_datetime
                )
            ]
            self._caches.reviews.set(reviews)
        return reviews

    async def _commits_from_db(self) -> list[github_types.CachedGitHubBranchCommit]:
        async with database.create_session() as session:
            db_commits_as_dicts = (
                await prcommit_model.PullRequestCommit.get_pull_request_commits(
                    session,
                    self.pull["id"],
                    self.pull["head"]["sha"],
                )
            )

        return [
            github_types.to_cached_github_branch_commit(commit)
            for commit in db_commits_as_dicts
        ]

    async def _commits_from_http(self) -> list[github_types.CachedGitHubBranchCommit]:
        return [
            github_types.to_cached_github_branch_commit(commit)
            async for commit in typing.cast(
                abc.AsyncIterable[github_types.GitHubBranchCommit],
                self.client.items(
                    f"{self.base_url}/pulls/{self.pull['number']}/commits",
                    resource_name="commits",
                    page_limit=5,
                ),
            )
        ]

    @property
    async def commits(self) -> list[github_types.CachedGitHubBranchCommit]:
        commits = self._caches.commits.get()
        if commits is cache.Unset:
            # Use pull request commits from db only if the pull request is already in db
            commits = []
            if await ghinpg_utils.can_repo_use_github_in_pg_data(
                repo_owner=self.repository.repo["owner"]["login"],
            ):
                commits = await self._commits_from_db()

            if not commits:
                commits = await self._commits_from_http()

            if len(commits) >= 250:
                self.log.warning("more than 250 commits found, is_behind maybe wrong")

            self._caches.commits.set(commits)
        return commits

    async def has_squashable_commits(self) -> bool:
        return any(
            commit.commit_message.startswith("squash!")
            or commit.commit_message.startswith("fixup!")
            or commit.commit_message.startswith("amend!")
            for commit in await self.commits
        )

    async def _files_from_http(self) -> list[github_types.CachedGitHubFile]:
        try:
            return [
                github_types.to_cached_github_file(file)
                async for file in typing.cast(
                    abc.AsyncIterable[github_types.GitHubFile],
                    self.client.items(
                        f"{self.base_url}/pulls/{self.pull['number']}/files",
                        resource_name="files",
                        page_limit=10,
                    ),
                )
            ]
        except http.HTTPClientSideError as e:
            if (
                e.status_code == 422
                and "Sorry, this diff is taking too long to generate" in e.message
            ):
                raise exceptions.UnprocessablePullRequestError(
                    "GitHub cannot generate the file list because the diff is taking too long",
                )
            raise

    async def _files_from_db(self) -> list[github_types.CachedGitHubFile]:
        async with database.create_session() as session:
            files_as_dict = await prfile_model.PullRequestFile.get_pull_request_files(
                session,
                self.pull["id"],
                self.pull["head"]["sha"],
            )

        return [github_types.to_cached_github_file(file) for file in files_as_dict]

    @property
    async def files(self) -> list[github_types.CachedGitHubFile]:
        files = self._caches.files.get()
        if files is cache.Unset:
            files = []
            if await ghinpg_utils.can_repo_use_github_in_pg_data(
                repo_owner=self.repository.repo["owner"]["login"],
            ):
                files = await self._files_from_db()

            if not files:
                # NOTE: A pull request can have no file modified in it, in which case
                # we will always make an HTTP call to GitHub just to be sure we didn't
                # miss anything.
                files = await self._files_from_http()

            self._caches.files.set(files)
        return files

    @property
    def closed(self) -> bool:
        # NOTE(sileht): GitHub automerge doesn't always close pull requests
        # when it merges them.
        return self.pull["state"] == "closed" or self.pull["merged"]

    @property
    def pull_from_fork(self) -> bool:
        if self.pull["head"]["repo"] is None:
            # Deleted fork repository
            return False
        return self.pull["head"]["repo"]["id"] != self.pull["base"]["repo"]["id"]

    def can_change_github_workflow(self) -> bool:
        workflows_perm = self.repository.installation.installation["permissions"].get(
            "workflows",
        )
        return workflows_perm == "write"

    def github_actions_controllable(self) -> bool:
        workflows_perm = self.repository.installation.installation["permissions"].get(
            "actions",
        )
        return workflows_perm == "write"

    async def github_workflow_changed(self) -> bool:
        for f in await self.files:
            if f["filename"].startswith(".github/workflows"):
                return True
        return False

    def user_refresh_requested(self) -> bool:
        return any(
            (
                source["event_type"] == "refresh"
                and typing.cast(github_types.GitHubEventRefresh, source["data"])[
                    "action"
                ]
                == "user"
            )
            or (
                source["event_type"] == "check_suite"
                and typing.cast(github_types.GitHubEventCheckSuite, source["data"])[
                    "action"
                ]
                == "rerequested"
                and typing.cast(github_types.GitHubEventCheckSuite, source["data"])[
                    "app"
                ]["id"]
                == settings.GITHUB_APP_ID
            )
            or (
                source["event_type"] == "check_run"
                and typing.cast(github_types.GitHubEventCheckRun, source["data"])[
                    "action"
                ]
                == "rerequested"
                and typing.cast(github_types.GitHubEventCheckRun, source["data"])[
                    "app"
                ]["id"]
                == settings.GITHUB_APP_ID
            )
            for source in self.sources
        )

    def admin_refresh_requested(self) -> bool:
        return any(
            (
                source["event_type"] == "refresh"
                and typing.cast(github_types.GitHubEventRefresh, source["data"])[
                    "action"
                ]
                == "admin"
            )
            for source in self.sources
        )

    async def post_comment(self, message: str) -> github_types.GitHubComment:
        resp = await self.client.post(
            f"{self.base_url}/issues/{self.pull['number']}/comments",
            json={"body": message},
        )
        return typing.cast(github_types.GitHubComment, resp.json())

    async def edit_comment(
        self,
        comment_id: github_types.GitHubCommentIdType,
        message: str,
    ) -> None:
        await self.client.post(
            f"{self.base_url}/issues/comments/{comment_id}",
            json={"body": message},
        )

    async def is_branch_protection_linear_history_enabled(self) -> bool:
        protection = await self.repository.get_branch_protection(
            self.pull["base"]["ref"],
        )
        return (
            protection is not None
            and "required_linear_history" in protection
            and protection["required_linear_history"]["enabled"]
        )
