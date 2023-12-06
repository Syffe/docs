import typing

import anys

from mergify_engine import github_types


def get_push_event_payload(
    ref: str | None = None,
    branch_name: str | None = None,
) -> dict[str, typing.Any]:
    payload: dict[
        str,
        typing.Any,
    ] = {}
    if ref:
        payload["ref"] = ref
    if branch_name:
        payload["ref"] = f"refs/heads/{branch_name}"

    return payload


def get_pull_request_event_payload(
    action: github_types.GitHubEventPullRequestActionType | None = None,
    pr_number: github_types.GitHubPullRequestNumber | None = None,
    merged: bool | None = None,
    base_ref: str | None = None,
) -> dict[str, typing.Any]:
    payload: dict[
        str,
        typing.Any,
    ] = {}
    if action is not None:
        payload["action"] = action
    if pr_number is not None:
        payload["number"] = pr_number
    if merged is not None or base_ref is not None:
        payload["pull_request"] = {}
        if merged is not None:
            payload["pull_request"]["merged"] = merged
        if base_ref is not None:
            payload["pull_request"]["base"] = {"ref": base_ref}

    return payload


def get_issue_comment_event_payload(
    action: github_types.GitHubEventIssueCommentActionType,
) -> dict[str, typing.Any]:
    payload: dict[str, typing.Any] = {
        "action": action,
    }

    return payload


def get_check_run_event_payload(
    action: github_types.GitHubCheckRunActionType | None = None,
    status: github_types.GitHubCheckRunStatus | None = None,
    conclusion: github_types.GitHubCheckRunConclusion | None = None,
    name: str | None = None,
    check_id: int | None = None,
    pr_number: github_types.GitHubPullRequestNumber | None = None,
) -> dict[str, typing.Any]:
    payload: dict[str, typing.Any] = {}

    if action:
        payload["action"] = action

    if status or conclusion or name or check_id:
        payload["check_run"] = {}
        if check_id:
            payload["check_run"]["id"] = check_id
        if status:
            payload["check_run"]["status"] = status
        if conclusion:
            payload["check_run"]["conclusion"] = conclusion
        if name:
            payload["check_run"]["name"] = name

    if pr_number:
        payload["pull_requests"] = anys.AnyContains(
            anys.AnyWithEntries({"number": pr_number}),
        )

    return payload


def get_pull_request_review_event_payload(
    action: github_types.GitHubEventPullRequestReviewActionType | None = None,
    state: github_types.GitHubEventReviewStateType | None = None,
) -> dict[str, typing.Any]:
    payload: dict[str, typing.Any] = {}
    if action:
        payload["action"] = action
    if state:
        payload["review"] = {"state": state}

    return payload
