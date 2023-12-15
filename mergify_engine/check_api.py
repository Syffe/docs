from collections import abc
import dataclasses
import datetime
import enum
import typing

import daiquiri

from mergify_engine import dashboard
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import settings
from mergify_engine import utils
from mergify_engine.clients import http


LOG = daiquiri.getLogger(__name__)

if typing.TYPE_CHECKING:
    from mergify_engine import context


# Used to track check run created by Mergify but for the user via the checks action
# e.g.: we want the engine to be retriggered if the state of this kind of checks changes.
USER_CREATED_CHECKS = "user-created-checkrun"


class GitHubCheckRunOutputParameters(typing.TypedDict, total=False):
    title: str
    summary: str
    text: str | None
    annotations: list[github_types.GitHubAnnotation] | None


class GitHubCheckRunParameters(typing.TypedDict, total=False):
    external_id: str
    head_sha: github_types.SHAType
    name: str
    status: github_types.GitHubCheckRunStatus
    output: GitHubCheckRunOutputParameters
    conclusion: github_types.GitHubCheckRunConclusion | None
    completed_at: github_types.ISODateTimeType
    started_at: github_types.ISODateTimeType
    details_url: str


@json.register_enum_type
class Conclusion(enum.Enum):
    PENDING = None
    CANCELLED = "cancelled"
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"
    NEUTRAL = "neutral"
    STALE = "stale"
    ACTION_REQUIRED = "action_required"
    TIMED_OUT = "timed_out"

    @staticmethod
    def _normalize(value: str) -> str:
        """Return normalized value."""
        return value.lower().replace("_", " ")

    @property
    def emoji(self) -> str | None:
        if self.value is None:
            return "🟠"
        if self.value == "success":
            return "✅"
        if self.value in ("failure", "timed_out"):
            return "❌"
        if self.value == "cancelled":
            return "🛑"
        if self.value in ("skipped", "neutral", "stale"):
            return "☑️"
        if self.value == "action_required":
            return "⚠️"

        return None

    def __str__(self) -> str:
        prefix = self.emoji
        prefix = "" if prefix is None else prefix + " "

        return prefix + self._normalize(self.name)


@dataclasses.dataclass
class Result:
    conclusion: Conclusion
    title: str
    summary: str
    annotations: list[github_types.GitHubAnnotation] | None = None
    started_at: datetime.datetime | None = None
    ended_at: datetime.datetime | None = None
    log_details: dict[str, typing.Any] | None = None
    details_url: str | None = None


def to_check_run_light(
    check: github_types.GitHubCheckRun,
) -> github_types.CachedGitHubCheckRun:
    if check["app"]["id"] != settings.GITHUB_APP_ID:
        # NOTE(sileht): We only need the output for our own checks
        check["output"]["text"] = None
        check["output"]["summary"] = ""
        check["output"]["annotations_url"] = ""

    return github_types.CachedGitHubCheckRun(
        {
            "id": check["id"],
            "app_id": check["app"]["id"],
            "app_name": check["app"]["name"],
            "app_avatar_url": check["app"]["owner"]["avatar_url"],
            "app_slug": check["app"]["slug"],
            "external_id": check["external_id"],
            "head_sha": check["head_sha"],
            "name": check["name"],
            "status": check["status"],
            "output": check["output"],
            "conclusion": check["conclusion"],
            "completed_at": check["completed_at"],
            "html_url": check["html_url"],
        },
    )


async def get_checks_for_ref(
    ctxt: "context.Context",
    sha: github_types.SHAType,
    check_name: str | None = None,
    app_id: int | None = None,
) -> list[github_types.CachedGitHubCheckRun]:
    params = {} if check_name is None else {"check_name": check_name}

    if app_id is not None:
        params["app_id"] = str(app_id)

    try:
        checks = [
            to_check_run_light(check)
            async for check in typing.cast(
                abc.AsyncGenerator[github_types.GitHubCheckRun, None],
                ctxt.client.items(
                    f"{ctxt.base_url}/commits/{sha}/check-runs",
                    resource_name="check runs",
                    page_limit=10,
                    api_version="antiope",
                    list_items="check_runs",
                    params=params,
                ),
            )
        ]
    except http.HTTPClientSideError as e:
        if e.status_code == 422 and "No commit found for SHA" in e.message:
            return []
        raise

    return checks


_K = typing.TypeVar("_K")
_V = typing.TypeVar("_V")


def compare_dict(d1: dict[_K, _V], d2: dict[_K, _V], keys: abc.Iterable[_K]) -> bool:
    return all(d1.get(key) == d2.get(key) for key in keys)


def check_need_update(
    previous_check: github_types.CachedGitHubCheckRun,
    expected_check: GitHubCheckRunParameters,
) -> bool:
    if compare_dict(
        typing.cast(dict[str, typing.Any], expected_check),
        typing.cast(dict[str, typing.Any], previous_check),
        ("head_sha", "status", "conclusion", "details_url"),
    ):
        # FIXME(sileht): according mypy and GitHub Doc this is impossible, but since
        # we write I prefer check in production before removing the runtime check
        if previous_check["output"] is None:
            LOG.error("GitHub check-run found with output=null", check=previous_check)  # type: ignore[unreachable]
        if expected_check["output"] is None:
            LOG.error("Generated check-run with output=null", check=expected_check)  # type: ignore[unreachable]

        if previous_check["output"] is None and expected_check["output"] is None:  # type: ignore[unreachable]
            return False  # type: ignore[unreachable]

        if previous_check["output"] is not None and compare_dict(
            typing.cast(dict[str, typing.Any], expected_check["output"]),
            typing.cast(dict[str, typing.Any], previous_check["output"]),
            ("title", "summary"),
        ):
            return False

    return True


async def set_check_run(
    ctxt: "context.Context",
    name: str,
    result: Result,
    external_id: str | None = None,
    skip_cache: bool = False,
) -> github_types.CachedGitHubCheckRun:
    status: github_types.GitHubCheckRunStatus
    status = "in_progress" if result.conclusion is Conclusion.PENDING else "completed"

    started_at = (result.started_at or date.utcnow()).isoformat()

    details_url = result.details_url
    if details_url is None:
        details_url = dashboard.get_eventlogs_url(
            ctxt.repository.installation.owner_login,
            ctxt.repository.repo["name"],
            ctxt.pull["number"],
        )

    post_parameters = GitHubCheckRunParameters(
        {
            "name": name,
            "head_sha": ctxt.pull["head"]["sha"],
            "status": status,
            "started_at": typing.cast(github_types.ISODateTimeType, started_at),
            "details_url": details_url,
            "output": {
                "title": result.title,
                "summary": result.summary,
            },
        },
    )

    # please mypy
    if post_parameters["output"] is None:
        raise RuntimeError("just set output is empty")

    if result.annotations is not None:
        post_parameters["output"]["annotations"] = result.annotations

    # Maximum output/summary length for Check API is 65535
    summary = post_parameters["output"]["summary"]
    if summary:
        post_parameters["output"]["summary"] = utils.unicode_truncate(
            summary,
            65535,
            "…",
        )

    if external_id:
        post_parameters["external_id"] = external_id

    if status == "completed":
        ended_at = (result.ended_at or date.utcnow()).isoformat()
        post_parameters["conclusion"] = result.conclusion.value
        post_parameters["completed_at"] = typing.cast(
            github_types.ISODateTimeType,
            ended_at,
        )

    if skip_cache:
        checks = sorted(
            await get_checks_for_ref(
                ctxt,
                ctxt.pull["head"]["sha"],
                check_name=name,
                app_id=settings.GITHUB_APP_ID,
            ),
            key=lambda c: c["id"],
            reverse=True,
        )
    else:
        checks = sorted(
            (c for c in await ctxt.pull_engine_check_runs if c["name"] == name),
            key=lambda c: c["id"],
            reverse=True,
        )

    if len(checks) >= 2:
        ctxt.log.warning(
            "pull requests with duplicate checks",
            checks=checks,
            skip_cache=skip_cache,
            all_checks=await ctxt.pull_engine_check_runs,
            fresh_checks=await get_checks_for_ref(
                ctxt,
                ctxt.pull["head"]["sha"],
                app_id=settings.GITHUB_APP_ID,
            ),
        )

    if not checks or (checks[0]["status"] == "completed" and status == "in_progress"):
        # NOTE(sileht): First time we see it, or the previous one have been completed and
        # now go back to in_progress. Since GitHub doesn't allow to change status of
        # completed check-runs, we have to create a new one.
        new_check = to_check_run_light(
            typing.cast(
                github_types.GitHubCheckRun,
                (
                    await ctxt.client.post(
                        f"{ctxt.base_url}/check-runs",
                        api_version="antiope",
                        json=post_parameters,
                    )
                ).json(),
            ),
        )

    # Don't do useless update
    elif check_need_update(checks[0], post_parameters):
        new_check = to_check_run_light(
            typing.cast(
                github_types.GitHubCheckRun,
                (
                    await ctxt.client.patch(
                        f"{ctxt.base_url}/check-runs/{checks[0]['id']}",
                        api_version="antiope",
                        json=post_parameters,
                    )
                ).json(),
            ),
        )

    else:
        new_check = checks[0]

    if not skip_cache:
        await ctxt.update_cached_check_runs(new_check)
    return new_check
