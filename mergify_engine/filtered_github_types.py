import typing

from mergify_engine import date
from mergify_engine import utils


def extract(event_type: str, event_id: str | None, event: typing.Any) -> typing.Any:
    mask: utils.Mask = {
        "sender": {
            "id": True,
            "login": True,
            "type": True,
        },
    }

    if event_type == "status":
        # To get PR from sha
        mask.update(
            {
                "sha": True,
                "context": True,
                # NOTE(sileht): only used for logging purpose
                "state": True,
            }
        )
    elif event_type == "pull_request_review":
        # NOTE(sileht): only used for logging purpose
        mask["action"] = True

    elif event_type == "refresh":
        # To get PR from sha or branch name
        mask.update(
            {
                "action": True,
                "ref": True,
                "pull_request_number": True,
                "source": True,
                "flag": True,
                "attempts": True,
            }
        )

    elif event_type == "push":
        # To get PR from sha
        mask.update(
            {
                "ref": True,
                "before": True,
                "after": True,
                "pusher": True,
            }
        )

    elif event_type in ("check_suite", "check_run"):
        # FIXME(sileht): GitHubEventCheckSuite/GitHubEventCheckRun looks badly typed.
        # code should use the app attributes inside check_suite/check_run.
        event["app"] = {"id": event[event_type]["app"]["id"]}

        # To get PR from sha
        mask.update(
            {
                "action": True,
                "app": {"id": True},
                event_type: {
                    "app": {"id": True},
                    "head_sha": True,
                    "pull_requests": [
                        {"number": True, "base": {"repo": {"id": True, "url": True}}}
                    ],
                },
            }
        )
        if event_type == "check_run":
            # NOTE(sileht): only used for logging purpose
            typing.cast(utils.Mask, mask["check_run"]).update(
                {
                    "name": True,
                    "id": True,
                    "conclusion": True,
                    "status": True,
                }
            )

    elif event_type == "pull_request":
        # For pull_request opened/synchronize/closed
        mask["action"] = True
        if event["action"] == "synchronize":
            mask.update(
                {
                    "before": True,
                    "after": True,
                }
            )

    elif event_type == "issue_comment":
        # For commands runner
        mask["comment"] = True

    elif event_type == "workflow_run":
        mask.update(
            {
                "workflow_run": {
                    "id": True,
                    "workflow_id": True,
                    "event": True,
                    "triggering_actor": {"id": True, "login": True, "type": True},
                    "head_sha": True,
                    "run_attempt": True,
                    "repository": {
                        "id": True,
                        "name": True,
                        "owner": {"id": True, "login": True, "type": True},
                    },
                },
                "repository": {
                    "id": True,
                    "name": True,
                    "owner": {"id": True, "login": True, "type": True},
                    "private": True,
                    "default_branch": True,
                },
                "organization": {"login": True},
            }
        )
    elif event_type == "workflow_job":
        mask.update(
            {
                "workflow_job": {
                    "id": True,
                    "run_id": True,
                    "name": True,
                    "conclusion": True,
                    "started_at": True,
                    "completed_at": True,
                    "labels": True,
                    "run_attempt": True,
                    "steps": True,
                },
                "repository": {
                    "id": True,
                    "name": True,
                    "owner": {"id": True, "login": True, "type": True},
                    "private": True,
                    "default_branch": True,
                },
            }
        )

    slim_event = utils.filter_dict(event, mask)
    # Inject some data for debugging purpose
    slim_event["delivery_id"] = event_id
    slim_event["received_at"] = date.utcnow().isoformat()
    return slim_event
