# FIXME(sileht): https://github.com/Mergifyio/engine/pull/4644
# mypy: disable-error-code=unreachable
import dataclasses
import typing

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
import fastapi
import sentry_sdk

from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import count_seats
from mergify_engine import engine
from mergify_engine import filtered_github_types
from mergify_engine import github_types
from mergify_engine import pull_request_finder
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import utils
from mergify_engine import worker_pusher
from mergify_engine.ci import job_registries
from mergify_engine.ci import models as ci_models
from mergify_engine.ci import pull_registries
from mergify_engine.clients import github
from mergify_engine.engine import commands_runner
from mergify_engine.queue import utils as queue_utils


LOG = daiquiri.getLogger(__name__)


async def get_pull_request_head_sha_to_number_mapping(
    redis_cache: redis_utils.RedisCache,
    owner_id: github_types.GitHubAccountIdType,
    repo_id: github_types.GitHubRepositoryIdType,
    sha: github_types.SHAType,
) -> github_types.GitHubPullRequestNumber | None:
    ret = await redis_cache.get(
        context.Context.redis_last_summary_pulls_key(owner_id, repo_id, sha),
    )
    if ret is None:
        return None
    return github_types.GitHubPullRequestNumber(int(ret))


async def meter_event(
    event_type: github_types.GitHubEventType,
    event: github_types.GitHubEvent,
    mergify_bot: github_types.GitHubAccount,
) -> None:
    tags = [f"event_type:{event_type}"]

    if event_type == "pull_request":
        event = typing.cast(github_types.GitHubEventPullRequest, event)
        tags.append(f"action:{event['action']}")
        if event["action"] == "closed" and event["pull_request"]["merged"]:
            if (
                event["pull_request"]["merged_by"] is not None
                and event["pull_request"]["merged_by"]["id"] == mergify_bot["id"]
            ):
                tags.append("by_mergify")

    # TODO(sileht): is statsd async ?
    statsd.increment("github.events", tags=tags)


@dataclasses.dataclass
class IgnoredEvent(Exception):
    """Raised when an is ignored."""

    event_type: str
    event_id: str
    reason: str


@dataclasses.dataclass
class EventBase:
    event_type: str
    event_id: str
    event: github_types.GitHubEventWithRepository | github_types.GitHubEvent

    @property
    def slim_event(self) -> typing.Any:
        return filtered_github_types.extract(self.event_type, self.event_id, self.event)


@dataclasses.dataclass
class EventToProcess(EventBase):
    event: github_types.GitHubEventWithRepository
    pull_request_number: github_types.GitHubPullRequestNumber | None
    priority: worker_pusher.Priority | None = None

    def set_sentry_info(self) -> None:
        sentry_sdk.set_user({"username": self.event["repository"]["owner"]["login"]})
        sentry_sdk.set_tag("gh_owner", self.event["repository"]["owner"]["login"])
        sentry_sdk.set_tag("gh_repo", self.event["repository"]["name"])

    def emit_log(self) -> None:
        LOG.info(
            "GithubApp event pushed",
            event_type=self.event_type,
            event_id=self.event_id,
            sender=self.event["sender"]["login"],
            gh_owner=self.event["repository"]["owner"]["login"],
            gh_repo=self.event["repository"]["name"],
            slim_event=self.slim_event,
            priority=self.priority,
            gh_pull=self.pull_request_number,
        )


@dataclasses.dataclass
class CIEventToProcess(EventBase):
    event: github_types.GitHubEventWorkflowRun | github_types.GitHubEventWorkflowJob

    def set_sentry_info(self) -> None:
        sentry_sdk.set_user({"username": self.event["repository"]["owner"]["login"]})
        sentry_sdk.set_tag("gh_owner", self.event["repository"]["owner"]["login"])
        sentry_sdk.set_tag("gh_repo", self.event["repository"]["name"])

    def emit_log(self) -> None:
        LOG.info(
            "GithubApp CI event pushed",
            event_type=self.event_type,
            event_id=self.event_id,
            sender=self.event["sender"]["login"],
            gh_owner=self.event["repository"]["owner"]["login"],
            gh_repo=self.event["repository"]["name"],
            slim_event=self.slim_event,
        )


@dataclasses.dataclass
class EventToIgnore(EventBase):
    reason: str

    def emit_log(self) -> None:
        if "repository" in self.event:
            event = typing.cast(github_types.GitHubEventWithRepository, self.event)
            gh_owner = event["repository"]["owner"]["login"]
            gh_repo = event["repository"]["name"]
        elif "organization" in self.event:
            gh_owner = self.event["organization"]["login"]
            gh_repo = None
        elif "installation" in self.event and "account" in self.event["installation"]:
            gh_owner = self.event["installation"]["account"]["login"]
            gh_repo = None
        else:
            gh_owner = None
            gh_repo = None

        LOG.info(
            "GithubApp event ignored",
            reason=self.reason,
            event_type=self.event_type,
            event_id=self.event_id,
            sender=self.event["sender"]["login"],
            gh_owner=gh_owner,
            gh_repo=gh_repo,
        )


async def clean_and_fill_caches(
    redis_links: redis_utils.RedisLinks,
    event_type: github_types.GitHubEventType,
    event_id: str,
    event: github_types.GitHubEvent,
) -> None:
    if event_type == "pull_request":
        event = typing.cast(github_types.GitHubEventPullRequest, event)
        if event["action"] in ("opened", "synchronize", "edited", "closed", "reopened"):
            await pull_request_finder.PullRequestFinder.sync(
                redis_links.cache, event["pull_request"]
            )

            commit_shas: set[github_types.SHAType] = (
                {event["before"], event["after"]}
                if event["action"] == "synchronize"
                else {event["pull_request"]["head"]["sha"]}
            )
            await pull_registries.RedisPullRequestRegistry.register_commits(
                redis_links.cache,
                event["pull_request"]["base"]["repo"]["owner"]["login"],
                event["pull_request"]["base"]["repo"]["name"],
                commit_shas,
                ci_models.PullRequest(
                    id=event["pull_request"]["id"],
                    number=event["pull_request"]["number"],
                    title=event["pull_request"]["title"],
                    state=event["pull_request"]["state"],
                ),
            )

        if event["action"] in ("opened", "edited"):
            await context.Repository.cache_pull_request_title(
                redis_links.cache,
                event["repository"]["id"],
                event["pull_request"]["number"],
                event["pull_request"]["title"],
            )

    elif event_type == "repository":
        event = typing.cast(github_types.GitHubEventRepository, event)
        if event["action"] in ("edited", "deleted"):
            await context.Repository.clear_config_file_cache(
                redis_links.cache, event["repository"]["id"]
            )

    elif event_type == "push":
        event = typing.cast(github_types.GitHubEventPush, event)
        if (
            f"refs/heads/{utils.extract_default_branch(event['repository'])}"
            == event["ref"]
        ):
            # NOTE(sileht): commits contains the list of commits returned by compare API
            # that by default returns only 20 commits
            # https://docs.github.com/en/developers/webhooks-and-events/webhooks/webhook-events-and-payloads#push
            # https://docs.github.com/en/rest/commits/commits#compare-two-commits
            if event["forced"] or len(event["commits"]) > 20:
                mergify_configuration_changed = True
            else:
                mergify_configuration_changed = False

                mergify_config_filenames = set(constants.MERGIFY_CONFIG_FILENAMES)
                commits = event["commits"].copy()
                if event["head_commit"] is not None:
                    commits.insert(0, event["head_commit"])
                for commit in commits:
                    if (
                        set(commit["added"]) & mergify_config_filenames
                        or set(commit["modified"]) & mergify_config_filenames
                        or set(commit["removed"]) & mergify_config_filenames
                    ):
                        mergify_configuration_changed = True
                        break

            if mergify_configuration_changed:
                await context.Repository.clear_config_file_cache(
                    redis_links.cache, event["repository"]["id"]
                )

    elif event_type == "organization":
        event = typing.cast(github_types.GitHubEventOrganization, event)
        if event["action"] == "deleted":
            await context.Installation.clear_team_members_cache_for_org(
                redis_links.team_members_cache, event["organization"]
            )
            await context.Repository.clear_team_permission_cache_for_org(
                redis_links.team_permissions_cache, event["organization"]
            )

        if event["action"] in ("deleted", "member_added", "member_removed"):
            await context.Repository.clear_user_permission_cache_for_org(
                redis_links.user_permissions_cache, event["organization"]
            )

    elif event_type == "member":
        event = typing.cast(github_types.GitHubEventMember, event)
        await context.Repository.clear_user_permission_cache_for_user(
            redis_links.user_permissions_cache,
            event["repository"]["owner"],
            event["repository"],
            event["member"],
        )

    elif event_type == "membership":
        event = typing.cast(github_types.GitHubEventMembership, event)
        if "slug" in event["team"]:
            await context.Installation.clear_team_members_cache_for_team(
                redis_links.team_members_cache,
                event["organization"],
                event["team"]["slug"],
            )
            await context.Repository.clear_team_permission_cache_for_team(
                redis_links.team_permissions_cache,
                event["organization"],
                event["team"]["slug"],
            )
        else:
            # Deleted team
            await context.Installation.clear_team_members_cache_for_org(
                redis_links.team_members_cache,
                event["organization"],
            )
            await context.Repository.clear_team_permission_cache_for_org(
                redis_links.team_permissions_cache, event["organization"]
            )

        await context.Repository.clear_user_permission_cache_for_org(
            redis_links.user_permissions_cache, event["organization"]
        )

    elif event_type == "team":
        event = typing.cast(github_types.GitHubEventTeam, event)
        if event["action"] in ("edited", "deleted"):
            await context.Installation.clear_team_members_cache_for_team(
                redis_links.team_members_cache,
                event["organization"],
                event["team"]["slug"],
            )
            await context.Repository.clear_team_permission_cache_for_team(
                redis_links.team_permissions_cache,
                event["organization"],
                event["team"]["slug"],
            )

        if event["action"] in (
            "edited",
            "added_to_repository",
            "removed_from_repository",
            "deleted",
        ):
            if "repository" in event and event["repository"] is not None:
                await context.Repository.clear_user_permission_cache_for_repo(
                    redis_links.user_permissions_cache,
                    event["organization"],
                    event["repository"],
                )
                await context.Repository.clear_team_permission_cache_for_repo(
                    redis_links.team_permissions_cache,
                    event["organization"],
                    event["repository"],
                )
            else:
                await context.Repository.clear_user_permission_cache_for_org(
                    redis_links.user_permissions_cache, event["organization"]
                )
                await context.Repository.clear_team_permission_cache_for_org(
                    redis_links.team_permissions_cache, event["organization"]
                )

    elif event_type == "team_add":
        event = typing.cast(github_types.GitHubEventTeamAdd, event)
        await context.Repository.clear_user_permission_cache_for_repo(
            redis_links.user_permissions_cache,
            event["repository"]["owner"],
            event["repository"],
        )
        await context.Repository.clear_team_permission_cache_for_repo(
            redis_links.team_permissions_cache,
            event["organization"],
            event["repository"],
        )


async def event_preprocessing(
    background_tasks: fastapi.BackgroundTasks,
    redis_links: redis_utils.RedisLinks,
    event_type: github_types.GitHubEventType,
    event_id: str,
    event: github_types.GitHubEvent,
) -> None:
    if event_type == "pull_request":
        event = typing.cast(github_types.GitHubEventPullRequest, event)
        if event["action"] in ("opened", "synchronize"):
            background_tasks.add_task(
                engine.create_initial_summary, redis_links.cache, event
            )

    elif event_type == "issue_comment":
        event = typing.cast(github_types.GitHubEventIssueComment, event)
        match = commands_runner.COMMAND_MATCHER.search(event["comment"]["body"])
        if match:
            # NOTE(sileht): nothing important should happen in this hook as we don't retry it
            background_tasks.add_task(commands_runner.on_each_event, event)


async def event_classifier(
    redis_links: redis_utils.RedisLinks,
    event_type: github_types.GitHubEventType,
    event_id: str,
    event: github_types.GitHubEvent,
    mergify_bot: github_types.GitHubAccount,
) -> EventToIgnore | EventToProcess | CIEventToProcess:
    # NOTE(sileht): those events are only used by synack or cache cleanup/feed
    if event_type in (
        "installation",
        "installation_repositories",
        "member",
        "membership",
        "organization",
        "team",
        "team_add",
        "repository",
    ):
        return EventToIgnore(event_type, event_id, event, f"{event_type} event")

    if "repository" in event:
        event = typing.cast(github_types.GitHubEventWithRepository, event)
        if event["repository"]["archived"]:
            return EventToIgnore(event_type, event_id, event, "repository archived")

    if event_type == "pull_request":
        event = typing.cast(github_types.GitHubEventPullRequest, event)
        if (
            event["action"] == "edited"
            and event["sender"]["id"] == mergify_bot["id"]
            and (
                # NOTE(greesb): For retrocompatibility. To remove once there are no more
                # PR using this.
                event["pull_request"]["head"]["ref"].startswith(
                    constants.MERGE_QUEUE_BRANCH_PREFIX
                )
                or queue_utils.is_pr_body_a_merge_queue_pr(
                    event["pull_request"]["body"]
                )
            )
        ):
            return EventToIgnore(
                event_type,
                event_id,
                event,
                "mergify merge queue description update",
            )

        return EventToProcess(
            event_type, event_id, event, event["pull_request"]["number"]
        )

    if event_type == "refresh":
        event = typing.cast(github_types.GitHubEventRefresh, event)
        return EventToProcess(event_type, event_id, event, event["pull_request_number"])

    if event_type == "pull_request_review_comment":
        event = typing.cast(github_types.GitHubEventPullRequestReviewComment, event)
        return EventToProcess(
            event_type,
            event_id,
            event,
            event["pull_request"]["number"]
            if event["pull_request"] is not None
            else None,
        )

    if event_type == "pull_request_review":
        event = typing.cast(github_types.GitHubEventPullRequestReview, event)
        return EventToProcess(
            event_type, event_id, event, event["pull_request"]["number"]
        )

    if event_type == "pull_request_review_thread":
        event = typing.cast(github_types.GitHubEventPullRequestReviewThread, event)
        return EventToProcess(
            event_type, event_id, event, event["pull_request"]["number"]
        )

    if event_type == "issue_comment":
        event = typing.cast(github_types.GitHubEventIssueComment, event)
        if "pull_request" not in event["issue"]:
            return EventToIgnore(
                event_type, event_id, event, "comment is not on a pull request"
            )

        if event["action"] not in ("created", "edited"):
            return EventToIgnore(
                event_type, event_id, event, f"comment action is '{event['action']}'"
            )

        if (
            # When someone else edit our comment the user id is still us
            # but the sender id is the one that edited the comment
            event["comment"]["user"]["id"] == mergify_bot["id"]
            and event["sender"]["id"] == mergify_bot["id"]
        ):
            return EventToIgnore(event_type, event_id, event, "comment by Mergify[bot]")

        if (
            # At the moment there is no specific "action" key or event
            # for when someone hides a comment.
            # So we need all those checks to identify someone hiding the comment
            # of a bot to be able to not re-execute it.
            event["comment"]["user"]["id"] != event["sender"]["id"]
            and event["action"] == "edited"
            and event["changes"]["body"]["from"] == event["comment"]["body"]
        ):
            return EventToIgnore(event_type, event_id, event, "comment has been hidden")

        if not commands_runner.COMMAND_MATCHER.search(event["comment"]["body"]):
            return EventToIgnore(
                event_type, event_id, event, "comment is not a command"
            )

        return EventToProcess(
            event_type,
            event_id,
            event,
            github_types.GitHubPullRequestNumber(event["issue"]["number"]),
            priority=worker_pusher.Priority.immediate,
        )

    if event_type == "status":
        event = typing.cast(github_types.GitHubEventStatus, event)
        return EventToProcess(
            event_type,
            event_id,
            event,
            await get_pull_request_head_sha_to_number_mapping(
                redis_links.cache,
                event["repository"]["owner"]["id"],
                event["repository"]["id"],
                event["sha"],
            ),
        )

    if event_type == "push":
        event = typing.cast(github_types.GitHubEventPush, event)
        if not event["ref"].startswith("refs/heads/"):
            return EventToIgnore(event_type, event_id, event, f"push on {event['ref']}")
        return EventToProcess(
            event_type,
            event_id,
            event,
            None,
        )

    if event_type == "check_suite":
        event = typing.cast(github_types.GitHubEventCheckSuite, event)
        if event["action"] != "rerequested":
            return EventToIgnore(
                event_type, event_id, event, f"check_suite/{event['action']}"
            )

        if (
            event["check_suite"]["app"]["id"] == settings.GITHUB_APP_ID
            and event["action"] != "rerequested"
            and event["check_suite"].get("external_id") != check_api.USER_CREATED_CHECKS
        ):
            return EventToIgnore(event_type, event_id, event, "mergify check_suite")

        return EventToProcess(
            event_type,
            event_id,
            event,
            await get_pull_request_head_sha_to_number_mapping(
                redis_links.cache,
                event["repository"]["owner"]["id"],
                event["repository"]["id"],
                event["check_suite"]["head_sha"],
            ),
        )

    if event_type == "check_run":
        event = typing.cast(github_types.GitHubEventCheckRun, event)
        if (
            event[event_type]["app"]["id"] == settings.GITHUB_APP_ID
            and event["action"] != "rerequested"
            and event[event_type].get("external_id") != check_api.USER_CREATED_CHECKS
        ):
            return EventToIgnore(event_type, event_id, event, "mergify check_run")

        return EventToProcess(
            event_type,
            event_id,
            event,
            await get_pull_request_head_sha_to_number_mapping(
                redis_links.cache,
                event["repository"]["owner"]["id"],
                event["repository"]["id"],
                event["check_run"]["head_sha"],
            ),
        )

    if event_type == "workflow_run":
        event = typing.cast(github_types.GitHubEventWorkflowRun, event)
        if job_registries.HTTPJobRegistry.is_workflow_run_ignored(
            event["workflow_run"]
        ):
            return EventToIgnore(
                event_type, event_id, event, reason="workflow_run ignored"
            )
        return CIEventToProcess(event_type, event_id, event)

    if event_type == "workflow_job":
        event = typing.cast(github_types.GitHubEventWorkflowJob, event)
        if job_registries.HTTPJobRegistry.is_workflow_job_ignored(
            event["workflow_job"]
        ):
            return EventToIgnore(
                event_type, event_id, event, reason="workflow_job ignored"
            )
        return CIEventToProcess(event_type, event_id, event)

    return EventToIgnore(event_type, event_id, event, "unexpected event_type")


async def filter_and_dispatch(
    background_tasks: fastapi.BackgroundTasks,
    redis_links: redis_utils.RedisLinks,
    event_type: github_types.GitHubEventType,
    event_id: str,
    event: github_types.GitHubEvent,
) -> None:
    mergify_bot = await github.GitHubAppInfo.get_bot(redis_links.cache)
    await meter_event(event_type, event, mergify_bot)
    await count_seats.store_active_users(
        redis_links.active_users, event_type, event_id, event
    )

    classified_event = await event_classifier(
        redis_links, event_type, event_id, event, mergify_bot
    )

    if isinstance(classified_event, EventToProcess):
        classified_event.set_sentry_info()
        await clean_and_fill_caches(redis_links, event_type, event_id, event)
        await event_preprocessing(
            background_tasks, redis_links, event_type, event_id, event
        )

        await worker_pusher.push(
            redis_links.stream,
            classified_event.event["repository"]["owner"]["id"],
            classified_event.event["repository"]["owner"]["login"],
            classified_event.event["repository"]["id"],
            classified_event.event["repository"]["name"],
            classified_event.pull_request_number,
            event_type,
            classified_event.slim_event,
            classified_event.priority,
        )

    if settings.CI_EVENT_INGESTION and isinstance(classified_event, CIEventToProcess):
        classified_event.set_sentry_info()
        await worker_pusher.push_ci_event(
            redis_links.stream,
            classified_event.event["repository"]["owner"]["id"],
            classified_event.event["repository"]["id"],
            event_type,
            event_id,
            classified_event.slim_event,
        )

    classified_event.emit_log()

    if isinstance(classified_event, EventToIgnore):
        raise IgnoredEvent(event_type, event_id, classified_event.reason)
