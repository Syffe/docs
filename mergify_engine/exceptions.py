import dataclasses
import datetime
import re

from redis import exceptions as redis_exceptions

from mergify_engine.clients import http


@dataclasses.dataclass
class UnprocessablePullRequest(Exception):
    reason: str


class MergifyNotInstalled(Exception):
    pass


@dataclasses.dataclass
class RateLimited(Exception):
    countdown: datetime.timedelta
    remaining: int


@dataclasses.dataclass
class EngineNeedRetry(Exception):
    pass


RATE_LIMIT_RETRY_MIN = datetime.timedelta(seconds=3)

IGNORED_HTTP_ERROR_REASONS: dict[int, list[str]] = {451: ["dmca"]}
IGNORED_HTTP_ERROR_MESSAGES: dict[int, list[str | re.Pattern[str]]] = {
    403: [
        "Repository access blocked",  # Blocked Github Account or Repo
        "Resource not accessible by integration",  # missing permission
        "Repository was archived so is read-only",
        re.compile(
            r"Although you appear to have the correct authorization credentials, the `.*` organization has an IP allow list enabled, and .* is not permitted to access this resource\."
        ),
    ],
    422: [
        "Sorry, there was a problem generating this diff. The repository may be missing relevant data."
        "The request could not be processed because too many files changed.",
        "No commit found for SHA:",
    ],
    503: ["Sorry, this diff is taking too long to generate."],
}


def should_be_ignored(exception: Exception) -> bool:
    if isinstance(exception, http.HTTPClientSideError):
        for reasons in IGNORED_HTTP_ERROR_REASONS.get(exception.status_code, []):
            if exception.response.json().get("reason", "") in reasons:
                return True

    if isinstance(exception, (http.HTTPClientSideError, http.HTTPServerSideError)):
        for error in IGNORED_HTTP_ERROR_MESSAGES.get(exception.status_code, []):
            if isinstance(error, str):
                if error in exception.message:
                    return True
            elif isinstance(error, re.Pattern):
                if error.match(exception.message):
                    return True
            else:
                raise RuntimeError(
                    f"Unexpected IGNORED_HTTP_ERROR_MESSAGES datatype: {type(error)}"
                )

        # NOTE(sileht): a repository return 404 for /pulls..., so can't do much
        if exception.status_code == 404 and exception.request.url.path.endswith(
            "/pulls"
        ):
            return True

        # NOTE(sileht): branch is gone since we started to handle a PR
        elif exception.status_code == 404 and "/branches/" in str(
            exception.request.url
        ):
            return True

    return False


def need_retry(
    exception: Exception,
) -> datetime.timedelta | None:  # pragma: no cover
    if isinstance(exception, RateLimited):
        # NOTE(sileht): when we are close to reset date, and since utc time between us and
        # github differ a bit, we can have negative delta, so set a minimun for retrying
        return max(exception.countdown, RATE_LIMIT_RETRY_MIN)
    elif isinstance(exception, EngineNeedRetry):
        return datetime.timedelta(minutes=1)

    elif isinstance(exception, (http.RequestError, http.HTTPServerSideError)):
        # NOTE(sileht): We already retry locally with urllib3, so if we get there, Github
        # is in a really bad shape...
        return datetime.timedelta(minutes=1)

    # NOTE(sileht): Most of the times token are just temporary invalid, Why ?
    # no idea, ask Github...
    elif isinstance(exception, http.HTTPClientSideError):
        # Bad creds or token expired, we can't really known
        if exception.response.status_code == 401:
            return datetime.timedelta(minutes=1)
        # Rate limit or abuse detection mechanism, futures events will be rate limited
        # correctly by mergify_engine.utils.Github()
        elif exception.response.status_code == 403:
            return datetime.timedelta(minutes=3)

    elif isinstance(exception, redis_exceptions.ResponseError):
        # Redis script bug or OOM
        return datetime.timedelta(minutes=1)

    elif isinstance(exception, redis_exceptions.ConnectionError):
        # Redis down
        return datetime.timedelta(minutes=1)

    return None
