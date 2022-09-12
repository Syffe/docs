import dataclasses
import typing

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
import sentry_sdk

from mergify_engine import check_api
from mergify_engine import config
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import count_seats
from mergify_engine import engine
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import worker_pusher
from mergify_engine.engine import commands_runner
from mergify_engine.queue import utils as queue_utils


LOG = daiquiri.getLogger(__name__)


def set_sentry_info(owner_login: str, repo_name: str | None) -> None:
    sentry_sdk.set_user({"username": owner_login})
    sentry_sdk.set_tag("gh_owner", owner_login)
    if repo_name is not None:
        sentry_sdk.set_tag("gh_repo", repo_name)


async def get_pull_request_head_sha_to_number_mapping(
    redis_cache: redis_utils.RedisCache,
    owner_id: github_types.GitHubAccountIdType,
    repo_id: github_types.GitHubRepositoryIdType,
    sha: github_types.SHAType,
) -> typing.Optional[github_types.GitHubPullRequestNumber]:
    ret = await redis_cache.get(
        context.Context.redis_last_summary_pulls_key(owner_id, repo_id, sha),
    )
    if ret is None:
        return None
    return github_types.GitHubPullRequestNumber(int(ret))


def meter_event(
    event_type: github_types.GitHubEventType, event: github_types.GitHubEvent
) -> None:
    tags = [f"event_type:{event_type}"]

    if event_type == "pull_request":
        event = typing.cast(github_types.GitHubEventPullRequest, event)
        tags.append(f"action:{event['action']}")
        if event["action"] == "closed" and event["pull_request"]["merged"]:
            if (
                event["pull_request"]["merged_by"] is not None
                and event["pull_request"]["merged_by"]["login"] == config.BOT_USER_LOGIN
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


def _log_on_exception(exc: Exception, msg: str) -> None:
    if exceptions.should_be_ignored(exc) or exceptions.need_retry(exc):
        log = LOG.debug
    else:
        log = LOG.error
    log(msg, exc_info=exc)


async def push_to_worker(
    redis_links: redis_utils.RedisLinks,
    event_type: github_types.GitHubEventType,
    event_id: str,
    event: github_types.GitHubEvent,
) -> None:

    pull_number = None
    ignore_reason = None

    if event_type == "pull_request":
        event = typing.cast(github_types.GitHubEventPullRequest, event)
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_id = event["repository"]["id"]
        repo_name = event["repository"]["name"]
        pull_number = event["pull_request"]["number"]
        set_sentry_info(owner_login, repo_name)

        if event["action"] in ("opened", "edited"):
            await context.Repository.cache_pull_request_title(
                redis_links.cache,
                repo_id,
                pull_number,
                event["pull_request"]["title"],
            )
        if event["repository"]["archived"]:
            ignore_reason = "repository archived"

        elif event["action"] in ("opened", "synchronize"):
            try:
                await engine.create_initial_summary(redis_links.cache, event)
            except Exception as e:
                _log_on_exception(e, "fail to create initial summary")
        elif (
            event["action"] == "edited"
            and event["sender"]["id"] == config.BOT_USER_ID
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
            ignore_reason = "mergify merge-queue description update"

    elif event_type == "repository":
        event = typing.cast(github_types.GitHubEventRepository, event)
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_id = event["repository"]["id"]
        repo_name = event["repository"]["name"]
        set_sentry_info(owner_login, repo_name)

        if event["action"] in ("edited", "deleted"):
            await context.Repository.clear_config_file_cache(redis_links.cache, repo_id)
        ignore_reason = "unused repository event"

    elif event_type == "refresh":
        event = typing.cast(github_types.GitHubEventRefresh, event)
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_id = event["repository"]["id"]
        repo_name = event["repository"]["name"]
        set_sentry_info(owner_login, repo_name)

        if event["pull_request_number"] is not None:
            pull_number = event["pull_request_number"]

    elif event_type == "pull_request_review_comment":
        event = typing.cast(github_types.GitHubEventPullRequestReviewComment, event)
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_id = event["repository"]["id"]
        repo_name = event["repository"]["name"]
        set_sentry_info(owner_login, repo_name)

        if event["pull_request"] is not None:
            pull_number = event["pull_request"]["number"]

        if event["repository"]["archived"]:
            ignore_reason = "repository archived"

    elif event_type == "pull_request_review":
        event = typing.cast(github_types.GitHubEventPullRequestReview, event)
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_id = event["repository"]["id"]
        repo_name = event["repository"]["name"]
        pull_number = event["pull_request"]["number"]
        set_sentry_info(owner_login, repo_name)

    elif event_type == "pull_request_review_thread":
        event = typing.cast(github_types.GitHubEventPullRequestReviewThread, event)
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_id = event["repository"]["id"]
        repo_name = event["repository"]["name"]
        pull_number = event["pull_request"]["number"]
        set_sentry_info(owner_login, repo_name)

    elif event_type == "issue_comment":
        event = typing.cast(github_types.GitHubEventIssueComment, event)
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_id = event["repository"]["id"]
        repo_name = event["repository"]["name"]
        pull_number = github_types.GitHubPullRequestNumber(event["issue"]["number"])
        set_sentry_info(owner_login, repo_name)

        if event["repository"]["archived"]:
            ignore_reason = "repository archived"

        elif "pull_request" not in event["issue"]:
            ignore_reason = "comment is not on a pull request"

        elif event["action"] not in ("created", "edited"):
            ignore_reason = f"comment action is '{event['action']}'"

        elif (
            # When someone else edit our comment the user id is still us
            # but the sender id is the one that edited the comment
            event["comment"]["user"]["id"] == config.BOT_USER_ID
            and event["sender"]["id"] == config.BOT_USER_ID
        ):
            ignore_reason = "comment by Mergify[bot]"

        elif (
            # At the moment there is no specific "action" key or event
            # for when someone hides a comment.
            # So we need all those checks to identify someone hiding the comment
            # of a bot to be able to not re-execute it.
            event["comment"]["user"]["id"] != event["sender"]["id"]
            and event["action"] == "edited"
            and event["changes"]["body"]["from"] == event["comment"]["body"]
        ):
            ignore_reason = "comment has been hidden"

        else:
            # NOTE(sileht): nothing important should happen in this hook as we don't retry it
            try:
                await commands_runner.on_each_event(event)
            except Exception as e:
                _log_on_exception(e, "commands_runner.on_each_event failed")

    elif event_type == "status":
        event = typing.cast(github_types.GitHubEventStatus, event)
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_id = event["repository"]["id"]
        repo_name = event["repository"]["name"]
        set_sentry_info(owner_login, repo_name)

        if event["repository"]["archived"]:
            ignore_reason = "repository archived"

        pull_number = await get_pull_request_head_sha_to_number_mapping(
            redis_links.cache, owner_id, repo_id, event["sha"]
        )

    elif event_type == "push":
        event = typing.cast(github_types.GitHubEventPush, event)
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_id = event["repository"]["id"]
        repo_name = event["repository"]["name"]
        set_sentry_info(owner_login, repo_name)

        if event["repository"]["archived"]:
            ignore_reason = "repository archived"

        elif not event["ref"].startswith("refs/heads/"):
            ignore_reason = f"push on {event['ref']}"

        elif event["repository"]["archived"]:  # pragma: no cover
            ignore_reason = "repository archived"

        if f"refs/heads/{event['repository']['default_branch']}" == event["ref"]:

            # NOTE(sileht): commits contains the list of commits returned by compare API
            # that by default returns only 250 commits
            # https://docs.github.com/en/developers/webhooks-and-events/webhooks/webhook-events-and-payloads#push
            # https://docs.github.com/en/rest/commits/commits#compare-two-commits
            if event["forced"] or len(event["commits"]) > 250:
                mergify_configuration_changed = True
            else:
                mergify_configuration_changed = False

                mergify_config_filenames = set(constants.MERGIFY_CONFIG_FILENAMES)
                for commit in event["commits"]:
                    if (
                        set(commit["added"]) & mergify_config_filenames
                        or set(commit["modified"]) & mergify_config_filenames
                        or set(commit["removed"]) & mergify_config_filenames
                    ):
                        mergify_configuration_changed = True
                        break

            if mergify_configuration_changed:
                await context.Repository.clear_config_file_cache(
                    redis_links.cache, repo_id
                )

    elif event_type == "check_suite":
        event = typing.cast(github_types.GitHubEventCheckSuite, event)
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_id = event["repository"]["id"]
        repo_name = event["repository"]["name"]
        set_sentry_info(owner_login, repo_name)

        if event["repository"]["archived"]:
            ignore_reason = "repository archived"

        elif event["action"] != "rerequested":
            ignore_reason = f"check_suite/{event['action']}"

        elif (
            event[event_type]["app"]["id"] == config.INTEGRATION_ID
            and event["action"] != "rerequested"
            and event[event_type].get("external_id") != check_api.USER_CREATED_CHECKS
        ):
            ignore_reason = f"mergify {event_type}"

        pull_number = await get_pull_request_head_sha_to_number_mapping(
            redis_links.cache, owner_id, repo_id, event["check_suite"]["head_sha"]
        )

    elif event_type == "check_run":
        event = typing.cast(github_types.GitHubEventCheckRun, event)
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_id = event["repository"]["id"]
        repo_name = event["repository"]["name"]
        set_sentry_info(owner_login, repo_name)

        if event["repository"]["archived"]:
            ignore_reason = "repository archived"

        elif (
            event[event_type]["app"]["id"] == config.INTEGRATION_ID
            and event["action"] != "rerequested"
            and event[event_type].get("external_id") != check_api.USER_CREATED_CHECKS
        ):
            ignore_reason = f"mergify {event_type}"

        pull_number = await get_pull_request_head_sha_to_number_mapping(
            redis_links.cache, owner_id, repo_id, event["check_run"]["head_sha"]
        )

    elif event_type == "organization":
        event = typing.cast(github_types.GitHubEventOrganization, event)
        owner_login = event["organization"]["login"]
        owner_id = event["organization"]["id"]
        repo_name = None
        repo_id = None
        ignore_reason = "organization event"
        set_sentry_info(owner_login, repo_name)

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
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_id = event["repository"]["id"]
        repo_name = event["repository"]["name"]
        ignore_reason = "member event"
        set_sentry_info(owner_login, repo_name)

        await context.Repository.clear_user_permission_cache_for_user(
            redis_links.user_permissions_cache,
            event["repository"]["owner"],
            event["repository"],
            event["member"],
        )

    elif event_type == "membership":
        event = typing.cast(github_types.GitHubEventMembership, event)
        owner_login = event["organization"]["login"]
        owner_id = event["organization"]["id"]
        repo_name = None
        repo_id = None
        ignore_reason = "membership event"
        set_sentry_info(owner_login, repo_name)

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
        owner_login = event["organization"]["login"]
        owner_id = event["organization"]["id"]
        repo_id = None
        repo_name = None
        ignore_reason = "team event"
        set_sentry_info(owner_login, repo_name)

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
            if "repository" in event:
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
        owner_login = event["repository"]["owner"]["login"]
        owner_id = event["repository"]["owner"]["id"]
        repo_id = event["repository"]["id"]
        repo_name = event["repository"]["name"]
        ignore_reason = "team_add event"
        set_sentry_info(owner_login, repo_name)

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

    else:
        owner_login = "<unknown>"
        owner_id = "<unknown>"
        repo_name = "<unknown>"
        repo_id = "<unknown>"
        ignore_reason = "unexpected event_type"

    if ignore_reason is None:
        msg_action = "pushed to worker"
        slim_event = worker_pusher.extract_slim_event(event_type, event)

        await worker_pusher.push(
            redis_links.stream,
            owner_id,
            owner_login,
            repo_id,
            repo_name,
            pull_number,
            event_type,
            slim_event,
        )
    else:
        slim_event = None
        msg_action = f"ignored: {ignore_reason}"

    LOG.debug(
        "GithubApp event %s",
        msg_action,
        event_type=event_type,
        event_id=event_id,
        sender=event["sender"]["login"],
        gh_owner=owner_login,
        gh_repo=repo_name,
        event=slim_event,
    )

    if ignore_reason:
        raise IgnoredEvent(event_type, event_id, ignore_reason)


async def filter_and_dispatch(
    redis_links: redis_utils.RedisLinks,
    event_type: github_types.GitHubEventType,
    event_id: str,
    event: github_types.GitHubEvent,
) -> None:
    meter_event(event_type, event)
    await count_seats.store_active_users(redis_links.active_users, event_type, event)
    await push_to_worker(redis_links, event_type, event_id, event)


SHA_EXPIRATION = 60


def _get_github_pulls_from_sha(
    sha: github_types.SHAType,
    pulls: typing.List[github_types.GitHubPullRequest],
) -> typing.List[github_types.GitHubPullRequestNumber]:
    for pull in pulls:
        if pull["head"]["sha"] == sha:
            return [pull["number"]]
    return []


async def extract_pull_numbers_from_event(
    installation: context.Installation,
    event_type: github_types.GitHubEventType,
    data: github_types.GitHubEvent,
    opened_pulls: typing.List[github_types.GitHubPullRequest],
) -> typing.List[github_types.GitHubPullRequestNumber]:
    # NOTE(sileht): Don't fail if we received even on repo that doesn't exists anymore
    if event_type == "refresh":
        data = typing.cast(github_types.GitHubEventRefresh, data)
        if (pull_request_number := data.get("pull_request_number")) is not None:
            return [pull_request_number]
        elif (ref := data.get("ref")) is None:
            return [p["number"] for p in opened_pulls]
        else:
            branch = ref[11:]  # refs/heads/
            return [p["number"] for p in opened_pulls if p["base"]["ref"] == branch]
    elif event_type == "push":
        data = typing.cast(github_types.GitHubEventPush, data)
        branch = data["ref"][11:]  # refs/heads/
        return [p["number"] for p in opened_pulls if p["base"]["ref"] == branch]
    elif event_type == "status":
        data = typing.cast(github_types.GitHubEventStatus, data)
        return _get_github_pulls_from_sha(data["sha"], opened_pulls)
    elif event_type == "check_suite":
        data = typing.cast(github_types.GitHubEventCheckSuite, data)
        # NOTE(sileht): This list may contains Pull Request from another org/user fork...
        base_repo_url = (
            f"{config.GITHUB_REST_API_URL}/repos/{installation.owner_login}/"
        )
        pulls = [
            p["number"]
            for p in data[event_type]["pull_requests"]
            if p["base"]["repo"]["url"].startswith(base_repo_url)
        ]
        if not pulls:
            sha = data[event_type]["head_sha"]
            pulls = _get_github_pulls_from_sha(sha, opened_pulls)
        return pulls
    elif event_type == "check_run":
        data = typing.cast(github_types.GitHubEventCheckRun, data)
        # NOTE(sileht): This list may contains Pull Request from another org/user fork...
        base_repo_url = f"{config.GITHUB_REST_API_URL}/repos/{installation.owner_login}"
        pulls = [
            p["number"]
            for p in data[event_type]["pull_requests"]
            if p["base"]["repo"]["url"].startswith(base_repo_url)
        ]
        if not pulls:
            sha = data[event_type]["head_sha"]
            pulls = _get_github_pulls_from_sha(sha, opened_pulls)
        return pulls
    else:
        return []
