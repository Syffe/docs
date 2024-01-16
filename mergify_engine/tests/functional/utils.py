import typing

import anys

from mergify_engine import github_types


if typing.TYPE_CHECKING:
    from mergify_engine.tests.functional import event_reader as event_reader_import


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
    pr_number: github_types.GitHubPullRequestNumber,
    action: github_types.GitHubEventIssueCommentActionType,
    comment_body: str | None = None,
) -> dict[str, typing.Any]:
    payload: dict[str, typing.Any] = {
        "action": action,
        "issue": {"number": pr_number},
    }
    if comment_body:
        payload.update({"comment": {"body": comment_body}})

    return payload


def get_check_run_event_payload(
    action: github_types.GitHubCheckRunActionType | None = None,
    status: github_types.GitHubCheckRunStatus | None = None,
    conclusion: github_types.GitHubCheckRunConclusion | None = None,
    name: str | None = None,
    check_id: int | None = None,
    pr_number: github_types.GitHubPullRequestNumber | None = None,
    output_title: str | None = None,
    output_summary: str | None = None,
    head_sha: str | None = None,
) -> dict[str, typing.Any]:
    payload: dict[str, typing.Any] = {}
    if action:
        payload["action"] = action
    if (
        status  # noqa: PLR0916
        or conclusion
        or name
        or check_id
        or pr_number
        or output_title
        or output_summary
        or head_sha
    ):
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
            payload["check_run"]["pull_requests"] = anys.AnyContains(
                anys.AnyWithEntries({"number": pr_number}),
            )
        if head_sha:
            payload["check_run"]["head_sha"] = head_sha
        if output_title or output_summary:
            payload["check_run"]["output"] = {}
            if output_title:
                payload["check_run"]["output"]["title"] = output_title
            if output_summary:
                payload["check_run"]["output"]["summary"] = output_summary

    return payload


def get_pull_request_review_event_payload(
    action: github_types.GitHubEventPullRequestReviewActionType | None = None,
    state: github_types.GitHubEventReviewStateType | None = None,
    review_body: str | None = None,
) -> dict[str, typing.Any]:
    payload: dict[str, typing.Any] = {}
    if action:
        payload["action"] = action
    if state or review_body:
        payload["review"] = {}
        if state:
            payload["review"]["state"] = state
        if review_body:
            payload["review"]["body"] = review_body

    return payload


def get_workflow_run_event_payload(
    action: github_types.GitHubEventWorkflowRunActionType,
    status: github_types.GitHubWorkflowRunStatusType | None = None,
    name: str | None = None,
) -> dict[str, typing.Any]:
    payload: dict[str, typing.Any] = {
        "action": action,
    }

    if name or status:
        payload["workflow_run"] = {}
        if name:
            payload["workflow_run"]["name"] = name
        if status:
            payload["workflow_run"]["status"] = status

    return payload


def match_expected_data(
    data: github_types.GitHubEvent,
    expected_data: typing.Any,
) -> bool:
    if isinstance(expected_data, dict):
        for key, expected in expected_data.items():
            if key not in data:
                return False
            if not match_expected_data(data[key], expected):  # type: ignore[literal-required]
                return False
        return True

    return bool(data == expected_data)


def sort_events(
    events_requested: list["event_reader_import.WaitForAllEvent"],
    events_received: list["event_reader_import.EventReceived"],
) -> list["event_reader_import.EventReceived"]:
    sorted_events = []
    used_event_received_indexes = set()
    for event_asked in events_requested:
        for idx2, event_received in enumerate(events_received):
            if (
                idx2 not in used_event_received_indexes
                and event_asked["event_type"] == event_received.event_type
                and match_expected_data(
                    event_received.event,
                    event_asked["payload"],
                )
            ):
                sorted_events.append(event_received)
                used_event_received_indexes.add(idx2)
                break
        else:
            raise RuntimeError("Did not manage to sort the list properly somehow")

    return sorted_events


def remove_useless_links(data: typing.Any) -> typing.Any:
    if isinstance(data, dict):
        data.pop("installation", None)
        data.pop("sender", None)
        data.pop("repository", None)
        data.pop("id", None)
        data.pop("node_id", None)
        data.pop("tree_id", None)
        data.pop("repo", None)
        data.pop("organization", None)
        data.pop("pusher", None)
        data.pop("_links", None)
        data.pop("user", None)
        data.pop("body", None)
        data.pop("after", None)
        data.pop("before", None)
        data.pop("app", None)
        data.pop("timestamp", None)
        data.pop("external_id", None)

        if "check_run" in data:
            data["check_run"].pop("check_suite", None)

        for key, value in list(data.items()):
            if key.endswith(("url", "_at")):
                del data[key]
            else:
                data[key] = remove_useless_links(value)

        return data

    if isinstance(data, list):
        return [remove_useless_links(elem) for elem in data]

    return data
