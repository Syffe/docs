import dataclasses
import typing

import daiquiri
import first

from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import dashboard
from mergify_engine import date
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.clients import github_app
from mergify_engine.clients import http
from mergify_engine.engine import actions_runner
from mergify_engine.engine import commands_runner
from mergify_engine.engine import queue_runner
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules.config import mergify as mergify_conf


LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class MultipleConfigurationFileFound(Exception):
    filenames: set[str]


async def _check_configuration_changes(
    ctxt: context.Context,
    current_mergify_config_file: context.MergifyConfigFile | None,
) -> bool:
    if (
        utils.extract_default_branch(ctxt.pull["base"]["repo"])
        != ctxt.pull["base"]["ref"]
    ):
        return False

    if ctxt.closed:
        # merge_commit_sha is a merge between the PR and the base branch only when the pull request is open
        # after it's None or the resulting commit of the pull request merge (maybe a rebase, squash, merge).
        # As the PR is closed, we don't care about the config change detector.
        return False

    # NOTE(sileht): This heuristic works only if _ensure_summary_on_head_sha()
    # is called after _check_configuration_changes().
    # If we don't have the real summary yet it means the pull request has just
    # been open or synchronize or we never see it.
    summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
    if summary and summary["output"]["title"] not in (
        constants.INITIAL_SUMMARY_TITLE,
        constants.CONFIGURATION_MUTIPLE_FOUND_SUMMARY_TITLE,
    ):
        if await ctxt.get_engine_check_run(constants.CONFIGURATION_CHANGED_CHECK_NAME):
            return True
        if await ctxt.get_engine_check_run(constants.CONFIGURATION_DELETED_CHECK_NAME):
            return True
        return False

    # NOTE(sileht): pull.base.sha is unreliable as its the sha when the PR is
    # open and not the merge-base/fork-point. So we compare the configuration from the base
    # branch with the one of the merge commit. If the configuration is changed by the PR, they will be
    # different.
    modified_config_files = [
        f
        for f in await ctxt.files
        if f["filename"] in constants.MERGIFY_CONFIG_FILENAMES
        and f["status"] not in ("removed", "unchanged")
    ]

    deleted_config_files = [
        f
        for f in await ctxt.files
        if (
            f["filename"] in constants.MERGIFY_CONFIG_FILENAMES
            and f["status"] == "removed"
        )
        or (
            f["status"] == "renamed"
            and f["previous_filename"] in constants.MERGIFY_CONFIG_FILENAMES
            and f["filename"] not in constants.MERGIFY_CONFIG_FILENAMES
        )
    ]

    if not modified_config_files and deleted_config_files:
        await check_api.set_check_run(
            ctxt,
            constants.CONFIGURATION_DELETED_CHECK_NAME,
            check_api.Result(
                check_api.Conclusion.SUCCESS,
                title="The Mergify configuration has been deleted",
                summary="Mergify will still continue to listen to commands.",
            ),
        )
        return True

    if not modified_config_files:
        return False

    config_filenames = {f["filename"] for f in modified_config_files}
    if current_mergify_config_file is not None:
        config_filenames.add(current_mergify_config_file["path"])

    if len(config_filenames) >= 2:
        raise MultipleConfigurationFileFound(config_filenames)

    if len(modified_config_files) != 1:
        raise RuntimeError(
            "modified_config_files must have only one element at this point"
        )

    future_mergify_config_file = modified_config_files[0]

    if (
        current_mergify_config_file is not None
        and current_mergify_config_file["path"]
        == future_mergify_config_file["filename"]
        and current_mergify_config_file["sha"] == future_mergify_config_file["sha"]
    ):
        # Nothing change between main branch and the pull request
        return False

    config_content = typing.cast(
        github_types.GitHubContentFile,
        await ctxt.client.item(future_mergify_config_file["contents_url"]),
    )

    try:
        await mergify_conf.get_mergify_config_from_file(
            ctxt.repository, context.content_file_to_config_file(config_content)
        )
    except mergify_conf.InvalidRules as e:
        # Not configured, post status check with the error message
        await check_api.set_check_run(
            ctxt,
            constants.CONFIGURATION_CHANGED_CHECK_NAME,
            check_api.Result(
                check_api.Conclusion.FAILURE,
                title="The new Mergify configuration is invalid",
                summary=str(e),
                annotations=e.get_annotations(e.filename),
            ),
        )
    else:
        await check_api.set_check_run(
            ctxt,
            constants.CONFIGURATION_CHANGED_CHECK_NAME,
            check_api.Result(
                check_api.Conclusion.SUCCESS,
                title="The new Mergify configuration is valid",
                summary="This pull request may require to be merged manually.",
            ),
        )

    return True


async def _get_summary_from_sha(
    ctxt: context.Context, sha: github_types.SHAType
) -> github_types.CachedGitHubCheckRun | None:
    return first.first(
        await check_api.get_checks_for_ref(
            ctxt,
            sha,
            check_name=constants.SUMMARY_NAME,
        ),
        key=lambda c: c["app_id"] == settings.GITHUB_APP_ID,
    )


async def _get_summary_from_synchronize_event(
    ctxt: context.Context,
) -> github_types.CachedGitHubCheckRun | None:
    synchronize_events = {
        typing.cast(github_types.GitHubEventPullRequest, s["data"])[
            "after"
        ]: typing.cast(github_types.GitHubEventPullRequest, s["data"])
        for s in ctxt.sources
        if s["event_type"] == "pull_request"
        and typing.cast(github_types.GitHubEventPullRequest, s["data"])["action"]
        == "synchronize"
        and "after" in s["data"]
    }
    if synchronize_events:
        ctxt.log.debug("checking summary from synchronize events")

        # NOTE(sileht): We sometimes got multiple synchronize events in a row, that's not
        # always the last one that has the Summary, so we also look in older ones if
        # necessary.
        after_sha = ctxt.pull["head"]["sha"]
        while synchronize_events:
            sync_event = synchronize_events.pop(after_sha, None)
            if sync_event:
                previous_summary = await _get_summary_from_sha(
                    ctxt, sync_event["before"]
                )
                if previous_summary and actions_runner.load_conclusions_line(
                    ctxt, previous_summary
                ):
                    ctxt.log.debug("got summary from synchronize events")
                    return previous_summary

                after_sha = sync_event["before"]
            else:
                break
    return None


async def _ensure_summary_on_head_sha(ctxt: context.Context) -> None:
    if ctxt.has_been_opened():
        return

    sha = await ctxt.get_cached_last_summary_head_sha()
    if sha is not None:
        if sha == ctxt.pull["head"]["sha"]:
            ctxt.log.debug("head sha didn't changed, no need to copy summary")
            return

        ctxt.log.debug(
            "head sha changed need to copy summary", gh_pull_previous_head_sha=sha
        )

    previous_summary = None

    if sha is not None:
        ctxt.log.debug("checking summary from redis")
        previous_summary = await _get_summary_from_sha(ctxt, sha)
        if previous_summary is not None:
            ctxt.log.debug("got summary from redis")

    if previous_summary is None:
        # NOTE(sileht): If the cached summary sha expires and the next event we got for
        # a pull request is "synchronize" we will lose the summary. Most of the times
        # it's not a big deal, but if the pull request is queued for merge, it may
        # be stuck.
        previous_summary = await _get_summary_from_synchronize_event(ctxt)

    # Sync only if the external_id is the expected one
    if previous_summary and (
        previous_summary["external_id"] is None
        or previous_summary["external_id"] == ""
        or previous_summary["external_id"] == str(ctxt.pull["number"])
    ):
        await ctxt.set_summary_check(
            check_api.Result(
                check_api.Conclusion(previous_summary["conclusion"]),
                title=previous_summary["output"]["title"],
                summary=previous_summary["output"]["summary"],
            )
        )
    elif previous_summary:
        ctxt.log.info(
            "got a previous summary, but collision detected with another pull request",
            other_pull=previous_summary["external_id"],
        )


async def get_context_with_sha_collision(
    ctxt: context.Context, summary: github_types.CachedGitHubCheckRun | None
) -> context.Context | None:
    if not (
        summary
        and summary["external_id"] is not None
        and summary["external_id"] != ""
        and summary["external_id"] != str(ctxt.pull["number"])
    ):
        return None

    try:
        conflicting_ctxt = await ctxt.repository.get_pull_request_context(
            github_types.GitHubPullRequestNumber(int(summary["external_id"]))
        )
    except http.HTTPNotFound:
        return None
    else:
        # NOTE(sileht): allow to override the summary of another pull request
        # only if this one is closed, but this can still confuse users as the
        # check-runs created by merge/queue action will not be cleaned.
        # TODO(sileht): maybe cancel all other mergify engine check-runs in this case?
        if conflicting_ctxt.closed:
            return None

        # TODO(sileht): try to report that without check-runs/statuses to the user
        # and without spamming him with comment
        ctxt.log.info(
            "sha collision detected between pull requests",
            other_pull=summary["external_id"],
        )
        return conflicting_ctxt


async def report_sha_collision(
    ctxt: context.Context, conflicting_ctxt: context.Context
) -> None:
    if not (await conflicting_ctxt.get_warned_about_sha_collision()):
        try:
            comment = await ctxt.post_comment(
                (
                    ":warning: The sha of the head commit of this PR conflicts with "
                    f"#{conflicting_ctxt.pull['number']}. Mergify cannot evaluate rules on this PR. :warning:"
                ),
            )
        except http.HTTPClientSideError as e:
            LOG.warning(
                "Unable to post sha collision detection comment",
                status_code=e.status_code,
                error_message=e.message,
            )
        else:
            await conflicting_ctxt.set_warned_about_sha_collision(comment["url"])


class T_PayloadEventIssueCommentSource(typing.TypedDict):
    event_type: github_types.GitHubEventType
    data: github_types.GitHubEventIssueComment
    timestamp: str


async def run(
    ctxt: context.Context,
    sources: list[context.T_PayloadEventSource],
) -> check_api.Result | None:
    LOG.debug("engine get context")
    ctxt.log.debug("engine start processing context")

    ctxt.sources.extend(
        [source for source in sources if source["event_type"] != "issue_comment"]
    )

    permissions_need_to_be_updated = github_app.permissions_need_to_be_updated(
        ctxt.repository.installation.installation
    )
    if permissions_need_to_be_updated:
        return check_api.Result(
            check_api.Conclusion.FAILURE,
            title="Required GitHub permissions are missing.",
            summary=f"You can accept them at {settings.DASHBOARD_UI_FRONT_URL}",
        )

    if ctxt.pull["base"]["repo"]["private"]:
        if not ctxt.subscription.has_feature(subscription.Features.PRIVATE_REPOSITORY):
            ctxt.log.info(
                "mergify disabled: private repository", reason=ctxt.subscription.reason
            )
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                title="Cannot use Mergify on a private repository",
                summary=ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"]
                ),
            )
    else:
        if not ctxt.subscription.has_feature(subscription.Features.PUBLIC_REPOSITORY):
            ctxt.log.info(
                "mergify disabled: public repository", reason=ctxt.subscription.reason
            )
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                title="Cannot use Mergify on a public repository",
                summary=ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"]
                ),
            )

    config_file = await ctxt.repository.get_mergify_config_file()

    try:
        ctxt.configuration_changed = await _check_configuration_changes(
            ctxt, config_file
        )
    except MultipleConfigurationFileFound as e:
        files = "\n * " + "\n * ".join(f for f in e.filenames)
        # NOTE(sileht): This replaces the summary, so we will may lost the
        # state of queue/comment action. But since we can't choice which config
        # file we need to use... we can't do much.
        return check_api.Result(
            check_api.Conclusion.FAILURE,
            title=constants.CONFIGURATION_MUTIPLE_FOUND_SUMMARY_TITLE,
            summary=f"You must keep only one of these configuration files in the repository: {files}",
        )

    # BRANCH CONFIGURATION CHECKING
    try:
        mergify_config = await ctxt.repository.get_mergify_config()
    except mergify_conf.InvalidRules as e:  # pragma: no cover
        ctxt.log.info(
            "The Mergify configuration is invalid",
            summary=str(e),
            annotations=e.get_annotations(e.filename),
        )
        # Not configured, post status check with the error message
        for s in ctxt.sources:
            if s["event_type"] == "pull_request":
                event = typing.cast(github_types.GitHubEventPullRequest, s["data"])
                if event["action"] in ("opened", "synchronize"):
                    return check_api.Result(
                        check_api.Conclusion.FAILURE,
                        title="The current Mergify configuration is invalid",
                        summary=str(e),
                        annotations=e.get_annotations(e.filename),
                    )
        return None

    if not ctxt.pull["locked"]:
        ctxt.log.debug("engine run pending commands")
        await commands_runner.run_commands_tasks(ctxt, mergify_config)

    summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)

    conflicting_ctxt = await get_context_with_sha_collision(ctxt, summary)
    if conflicting_ctxt is not None:
        await report_sha_collision(ctxt, conflicting_ctxt)
        return None

    await _ensure_summary_on_head_sha(ctxt)

    if not ctxt.has_been_opened() and summary is None:
        ctxt.log.warning(
            "the pull request doesn't have a summary",
            head_sha=ctxt.pull["head"]["sha"],
        )

    ctxt.log.debug("engine handle actions")
    if queue_utils.is_merge_queue_pr(ctxt.pull):
        await queue_runner.handle(
            ctxt,
            mergify_config["queue_rules"],
            mergify_config["partition_rules"],
        )
        return None

    return await actions_runner.handle(
        ctxt,
        mergify_config["pull_request_rules"],
        mergify_config["queue_rules"],
        mergify_config["partition_rules"],
    )


@exceptions.log_and_ignore_exception("fail to create initial summary")
async def create_initial_summary(
    redis: redis_utils.RedisCache, event: github_types.GitHubEventPullRequest
) -> None:
    owner = event["repository"]["owner"]
    repo = event["pull_request"]["base"]["repo"]

    if not await redis.exists(
        context.Repository.get_config_file_cache_key(
            repo["id"],
        )
    ):
        # Mergify is probably not activated on this repo
        return

    if queue_utils.is_merge_queue_pr(event["pull_request"]):
        return

    # NOTE(sileht): It's possible that a "push" event creates a summary before we
    # received the pull_request/opened event.
    # So we check first if a summary does not already exists, to not post
    # the summary twice. Since this method can ran in parallel of the worker
    # this is not a 100% reliable solution, but if we post a duplicate summary
    # check_api.set_check_run() handle this case and update both to not confuse users.
    summary_exists = await context.Context.summary_exists(
        redis, owner["id"], repo["id"], event["pull_request"]
    )

    if summary_exists:
        return

    installation_json = await github.get_installation_from_account_id(owner["id"])
    async with github.aget_client(installation_json) as client:
        post_parameters = {
            "name": constants.SUMMARY_NAME,
            "head_sha": event["pull_request"]["head"]["sha"],
            "status": check_api.Status.IN_PROGRESS.value,
            "started_at": date.utcnow().isoformat(),
            "details_url": dashboard.get_eventlogs_url(
                owner["login"], repo["name"], event["pull_request"]["number"]
            ),
            "output": {
                "title": constants.INITIAL_SUMMARY_TITLE,
                "summary": "Be patient, the page will be updated soon.",
            },
            "external_id": str(event["pull_request"]["number"]),
        }
        try:
            await client.post(
                f"/repos/{event['pull_request']['base']['user']['login']}/{event['pull_request']['base']['repo']['name']}/check-runs",
                api_version="antiope",
                json=post_parameters,
            )
        except http.HTTPClientSideError as e:
            if e.status_code == 422 and "No commit found for SHA" in e.message:
                return
            raise
