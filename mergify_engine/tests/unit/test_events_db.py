import random
from unittest import mock

import anys
from freezegun import freeze_time
import pytest
import sqlalchemy
from sqlalchemy import func
import sqlalchemy.ext.asyncio

from mergify_engine import context
from mergify_engine import date
from mergify_engine import eventlogs
from mergify_engine import events_db
from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine.models import enumerations
from mergify_engine.models import events as evt_model
from mergify_engine.queue.merge_train import checks
from mergify_engine.rules.config import partition_rules


async def insert_event(
    fake_repository: context.Repository,
    event: signals.EventName,
    metadata: signals.EventMetadata,
    pull_request: int | None = 0,
) -> None:
    if pull_request is not None:
        pull_request = github_types.GitHubPullRequestNumber(random.randint(1, 100))

    await events_db.insert(
        event=event,
        repository=fake_repository.repo,
        pull_request=pull_request,
        metadata=metadata,
        trigger="Rule: my rule",
    )


async def assert_base_event(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    # base event inserted
    result = await db.execute(
        sqlalchemy.select(func.count()).select_from(evt_model.Event)
    )
    assert result.scalar() == 1

    # GithubRepository and account inserted
    event = await db.scalar(sqlalchemy.select(evt_model.Event))
    assert event is not None
    assert event.repository.id == fake_repository.repo["id"]
    assert event.repository.owner.id == fake_repository.repo["owner"]["id"]


async def test_event_not_supported(fake_repository: context.Repository) -> None:
    with pytest.raises(events_db.EventNotHandled) as e:
        await events_db.insert(
            event="event.not.supported",  # type: ignore [arg-type]
            repository=fake_repository.repo,
            pull_request=github_types.GitHubPullRequestNumber(random.randint(1, 100)),
            metadata={},
            trigger="Rule: my rule",
        )
    assert "Event 'event.not.supported' not supported in database" == str(e.value)


async def test_event_action_assign_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.assign",
        signals.EventAssignMetadata(
            {"added": ["leo", "charly", "guillaume"], "removed": ["damien", "fabien"]}
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionAssign))
    assert event is not None
    assert set(event.added) == {"leo", "charly", "guillaume"}
    assert set(event.removed) == {"damien", "fabien"}


async def test_event_post_check_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.post_check",
        signals.EventPostCheckMetadata(
            {
                "title": "Rule: my check (post_check)",
                "conclusion": "success",
                "summary": "abc" * 100,
            }
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionPostCheck))
    assert event is not None
    assert event.title == "Rule: my check (post_check)"
    assert event.conclusion == "success"


async def test_event_action_copy_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.copy",
        signals.EventCopyMetadata(
            {"to": "test_branch", "pull_request_number": 123, "conflicts": False}
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionCopy))
    assert event is not None
    assert event.to == "test_branch"
    assert event.pull_request_number == 123
    assert event.conflicts is False


async def test_event_action_comment_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.comment",
        signals.EventCommentMetadata(message="hello world"),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionComment))
    assert event is not None
    assert event.message == "hello world"


async def test_event_action_close_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.close",
        signals.EventCommentMetadata(message="goodbye world"),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionClose))
    assert event is not None
    assert event.message == "goodbye world"


async def test_event_action_delete_head_branch_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.delete_head_branch",
        signals.EventDeleteHeadBranchMetadata(branch="change_branch"),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionDeleteHeadBranch))
    assert event is not None
    assert event.branch == "change_branch"


async def test_event_action_dismiss_reviews_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.dismiss_reviews",
        signals.EventDismissReviewsMetadata(users=["leo", "charly", "guillaume"]),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionDismissReviews))
    assert event is not None
    assert set(event.users) == {"leo", "charly", "guillaume"}


async def test_event_action_backport_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.backport",
        signals.EventCopyMetadata(
            {"to": "stable_branch", "pull_request_number": 456, "conflicts": True}
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionBackport))
    assert event is not None
    assert event.to == "stable_branch"
    assert event.pull_request_number == 456
    assert event.conflicts is True


async def test_event_action_edit_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.edit",
        signals.EventEditMetadata(draft=True),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionEdit))
    assert event is not None
    assert event.draft is True


async def test_event_action_label_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.label",
        signals.EventLabelMetadata(
            {
                "added": ["manual merge", "skip changelog"],
                "removed": ["hotfix", "skip tests"],
            }
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionLabel))
    assert event is not None
    assert set(event.added) == {"manual merge", "skip changelog"}
    assert set(event.removed) == {"hotfix", "skip tests"}


async def test_event_action_merge_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.merge",
        signals.EventMergeMetadata(branch="merge_branch"),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionMerge))
    assert event is not None
    assert event.branch == "merge_branch"


async def test_event_action_github_actions_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    inputs: dict[str, str | int | bool] = {"tool": "wrench", "size": 14, "in_use": True}
    await insert_event(
        fake_repository,
        "action.github_actions",
        signals.EventGithubActionsMetadata(
            workflow="verify_tool.yaml",
            inputs=inputs,
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionGithubActions))
    assert event is not None
    assert event.workflow == "verify_tool.yaml"
    assert event.inputs == inputs


@freeze_time("2023-07-10T14:00:00", tz_offset=0)
async def test_event_action_queue_enter_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.queue.enter",
        signals.EventQueueEnterMetadata(
            {
                "queue_name": "default",
                "branch": "refactor_test",
                "position": 3,
                "queued_at": date.utcnow(),
                "partition_name": partition_rules.PartitionRuleName(
                    "default_partition"
                ),
            }
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionQueueEnter))
    assert event is not None
    assert event.queue_name == "default"
    assert event.branch == "refactor_test"
    assert event.position == 3
    assert event.queued_at.isoformat() == "2023-07-10T14:00:00+00:00"
    assert event.partition_name == "default_partition"

    # partition name is nullable
    await insert_event(
        fake_repository,
        "action.queue.enter",
        signals.EventQueueEnterMetadata(
            {
                "queue_name": "default",
                "branch": "refactor_test",
                "position": 3,
                "queued_at": date.utcnow(),
                "partition_name": None,
            }
        ),
    )


@freeze_time("2023-07-10T14:00:00", tz_offset=0)
async def test_event_action_queue_merged_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.queue.merged",
        signals.EventQueueMergedMetadata(
            {
                "queue_name": "default",
                "branch": "some_branch",
                "queued_at": date.utcnow(),
                "partition_names": [
                    partition_rules.PartitionRuleName("partA"),
                    partition_rules.PartitionRuleName("partB"),
                ],
            }
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionQueueMerged))
    assert event is not None
    assert event.queue_name == "default"
    assert event.branch == "some_branch"
    assert event.queued_at.isoformat() == "2023-07-10T14:00:00+00:00"
    assert set(event.partition_names) == {"partA", "partB"}


@freeze_time("2023-07-10T14:00:00", tz_offset=0)
async def test_event_action_queue_leave_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.queue.leave",
        signals.EventQueueLeaveMetadata(
            {
                "queue_name": "default",
                "branch": "refactor_test",
                "position": 3,
                "queued_at": date.utcnow(),
                "partition_name": partition_rules.DEFAULT_PARTITION_NAME,
                "merged": False,
                "reason": "Pull request ahead in queue failed to get merged",
                "seconds_waiting_for_schedule": 5,
                "seconds_waiting_for_freeze": 5,
            }
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionQueueLeave))
    assert event is not None
    assert event.queue_name == "default"
    assert event.branch == "refactor_test"
    assert event.position == 3
    assert event.queued_at.isoformat() == "2023-07-10T14:00:00+00:00"
    assert event.partition_name == partition_rules.DEFAULT_PARTITION_NAME
    assert event.merged is False
    assert event.reason == "Pull request ahead in queue failed to get merged"
    assert event.seconds_waiting_for_schedule == 5
    assert event.seconds_waiting_for_freeze == 5


async def test_events_with_no_metadata(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    events_set: set[signals.EventName] = {
        "action.squash",
        "action.rebase",
        "action.refresh",
        "action.requeue",
        "action.unqueue",
        "action.update",
    }
    for event in events_set:
        await insert_event(
            fake_repository,
            event,
            signals.EventNoMetadata(),
        )

    events_types = await db.scalars(sqlalchemy.select(evt_model.Event.type))
    assert set(events_types.all()) == events_set


@freeze_time("2023-07-17T14:00:00", tz_offset=0)
async def test_event_action_queue_checks_start_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    unsuccessful_check = checks.QueueCheck.Serialized(
        {
            "name": "ruff",
            "description": "Syntax check",
            "state": "failure",
            "url": None,
            "avatar_url": "some_url",
        }
    )

    await insert_event(
        fake_repository,
        "action.queue.checks_start",
        signals.EventQueueChecksStartMetadata(
            {
                "branch": "fix_hyperdrive_trigger",
                "partition_name": partition_rules.DEFAULT_PARTITION_NAME,
                "position": 3,
                "queue_name": "default",
                "queued_at": date.utcnow(),
                "speculative_check_pull_request": {
                    "number": 123,
                    "in_place": True,
                    "checks_timed_out": False,
                    "checks_conclusion": "failure",
                    "checks_started_at": date.utcnow(),
                    "checks_ended_at": date.utcnow(),
                    "unsuccessful_checks": [unsuccessful_check],
                },
            }
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionQueueChecksStart))
    assert event is not None
    assert event.branch == "fix_hyperdrive_trigger"
    assert event.partition_name == "__default__"
    assert event.queue_name == "default"
    assert event.queued_at == anys.ANY_AWARE_DATETIME
    spec_check_pr = event.speculative_check_pull_request
    assert spec_check_pr is not None
    assert spec_check_pr.number == 123
    assert spec_check_pr.in_place is True
    assert spec_check_pr.checks_timed_out is False
    assert spec_check_pr.checks_conclusion == "failure"
    assert spec_check_pr.checks_started_at == anys.ANY_AWARE_DATETIME
    assert spec_check_pr.checks_ended_at == anys.ANY_AWARE_DATETIME
    assert spec_check_pr.unsuccessful_checks == [unsuccessful_check]


@freeze_time("2023-07-17T14:00:00", tz_offset=0)
async def test_event_action_queue_checks_end_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    unsuccessful_check = checks.QueueCheck.Serialized(
        {
            "name": "trivy",
            "description": "Security check",
            "state": "failure",
            "url": None,
            "avatar_url": "some_url",
        }
    )

    await insert_event(
        fake_repository,
        "action.queue.checks_end",
        signals.EventQueueChecksEndMetadata(
            {
                "branch": "feature_branch",
                "partition_name": partition_rules.DEFAULT_PARTITION_NAME,
                "position": 3,
                "queue_name": "default",
                "queued_at": date.utcnow(),
                "aborted": True,
                "abort_code": "PR_DEQUEUED",
                "abort_reason": "Pull request has been dequeued.",
                "abort_status": "DEFINITIVE",
                "unqueue_code": None,
                "speculative_check_pull_request": {
                    "number": 456,
                    "in_place": True,
                    "checks_timed_out": False,
                    "checks_conclusion": "failure",
                    "checks_started_at": date.utcnow(),
                    "checks_ended_at": date.utcnow(),
                    "unsuccessful_checks": [unsuccessful_check],
                },
            }
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionQueueChecksEnd))
    assert event is not None
    assert event.branch == "feature_branch"
    assert event.partition_name == "__default__"
    assert event.queue_name == "default"
    assert event.queued_at == anys.ANY_AWARE_DATETIME
    assert event.aborted is True
    assert event.abort_code == "PR_DEQUEUED"
    assert event.abort_reason == "Pull request has been dequeued."
    assert event.abort_status == "DEFINITIVE"
    assert event.unqueue_code is None
    spec_check_pr = event.speculative_check_pull_request
    assert spec_check_pr is not None
    assert spec_check_pr.number == 456
    assert spec_check_pr.in_place is True
    assert spec_check_pr.checks_timed_out is False
    assert spec_check_pr.checks_conclusion == "failure"
    assert spec_check_pr.checks_started_at == anys.ANY_AWARE_DATETIME
    assert spec_check_pr.checks_ended_at == anys.ANY_AWARE_DATETIME
    assert spec_check_pr.unsuccessful_checks == [unsuccessful_check]


async def test_event_action_request_reviews_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.request_reviews",
        signals.EventRequestReviewsMetadata(
            {
                "reviewers": ["leo", "charly", "guillaume"],
                "team_reviewers": ["damien", "fabien"],
            }
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionRequestReviews))
    assert event is not None
    assert set(event.reviewers) == {"leo", "charly", "guillaume"}
    assert set(event.team_reviewers) == {"damien", "fabien"}


async def test_event_action_review_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.review",
        signals.EventReviewMetadata(
            {
                "type": "APPROVE",
                "reviewer": "John Doe",
                "message": "Looks good to me",
            }
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionReview))
    assert event is not None
    assert event.review_type == "APPROVE"
    assert event.reviewer == "John Doe"
    assert event.message == "Looks good to me"


async def test_event_queue_freeze_create_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "queue.freeze.create",
        signals.EventQueueFreezeCreateMetadata(
            {
                "queue_name": "hotfix",
                "reason": "Incident in production",
                "cascading": True,
                "created_by": {"id": 123456, "type": "user", "name": "krilin"},
            }
        ),
        pull_request=None,
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventQueueFreezeCreate))
    assert event is not None
    assert event.queue_name == "hotfix"
    assert event.reason == "Incident in production"
    assert event.cascading is True
    assert event.created_by.id == 123456
    assert event.created_by.type == "user"
    assert event.created_by.name == "krilin"


async def test_event_queue_freeze_update_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "queue.freeze.update",
        signals.EventQueueFreezeUpdateMetadata(
            {
                "queue_name": "hotfix",
                "reason": "Incident in production",
                "cascading": True,
                "updated_by": {"id": 567, "type": "user", "name": "sangoku"},
            }
        ),
        pull_request=None,
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventQueueFreezeUpdate))
    assert event is not None
    assert event.queue_name == "hotfix"
    assert event.reason == "Incident in production"
    assert event.cascading is True
    assert event.updated_by.id == 567
    assert event.updated_by.type == "user"
    assert event.updated_by.name == "sangoku"


async def test_event_queue_freeze_delete_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "queue.freeze.delete",
        signals.EventQueueFreezeDeleteMetadata(
            {
                "queue_name": "hotfix",
                "deleted_by": {"id": 454, "type": "user", "name": "freezer"},
            }
        ),
        pull_request=None,
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventQueueFreezeDelete))
    assert event is not None
    assert event.queue_name == "hotfix"
    assert event.deleted_by.id == 454
    assert event.deleted_by.type == "user"
    assert event.deleted_by.name == "freezer"


async def test_event_queue_pause_create_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "queue.pause.create",
        signals.EventQueuePauseCreateMetadata(
            {
                "reason": "Incident in production",
                "created_by": {"id": 145, "type": "user", "name": "vegeta"},
            }
        ),
        pull_request=None,
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventQueuePauseCreate))
    assert event is not None
    assert event.reason == "Incident in production"
    assert event.created_by.id == 145
    assert event.created_by.type == "user"
    assert event.created_by.name == "vegeta"


async def test_event_queue_pause_update_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "queue.pause.update",
        signals.EventQueuePauseUpdateMetadata(
            {
                "reason": "Incident in production",
                "updated_by": {"id": 145, "type": "user", "name": "sangohan"},
            }
        ),
        pull_request=None,
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventQueuePauseUpdate))
    assert event is not None
    assert event.reason == "Incident in production"
    assert event.updated_by.id == 145
    assert event.updated_by.type == "user"
    assert event.updated_by.name == "sangohan"


async def test_event_queue_pause_delete_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "queue.pause.delete",
        signals.EventQueuePauseDeleteMetadata(
            deleted_by={"id": 987, "type": "user", "name": "cell"},
        ),
        pull_request=None,
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventQueuePauseDelete))
    assert event is not None
    assert event.deleted_by.id == 987
    assert event.deleted_by.type == "user"
    assert event.deleted_by.name == "cell"


def test_all_known_events_supported() -> None:
    known_evt_models = set(events_db.EVENT_NAME_TO_MODEL)
    known_evt_names = set(eventlogs.SUPPORTED_EVENT_NAMES)
    # NOTE(lecrepont01): equality will be achieved after MRGFY-2461
    # assert known_evt_names == known_evt_models
    assert known_evt_names - known_evt_models == {
        "action.request_reviewers",
    }

    known_evt_enum_types = {
        e.value for e in enumerations.EventType.__members__.values()
    }
    # NOTE(lecrepont01): equality will be achieved after MRGFY-2461
    # assert known_evt_names == known_evt_enum_types,
    # "Type values inserted should be kept in sync with the known event names"
    assert known_evt_names - known_evt_enum_types == {
        "action.request_reviewers",
    }


async def test_event_as_dict(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "queue.pause.delete",
        signals.EventQueuePauseDeleteMetadata(
            deleted_by={"id": 987, "type": "user", "name": "cell"},
        ),
        pull_request=None,
    )
    event = await db.scalar(sqlalchemy.select(evt_model.EventQueuePauseDelete))

    assert event is not None
    assert event.as_dict() == {
        "id": 1,
        "deleted_by_id": 987,
        "type": enumerations.EventType.QueuePauseDelete,
        "received_at": mock.ANY,
        "pull_request": None,
        "trigger": "Rule: my rule",
        "repository_id": 0,
        "deleted_by": {
            "id": 987,
            "type": enumerations.GithubAuthenticatedActorType.USER,
            "name": "cell",
        },
        "repository": {
            "id": 0,
            "owner_id": 0,
            "name": "mergify-engine",
            "private": False,
            "default_branch": "main",
            "full_name": "Mergifyio/mergify-engine",
            "archived": False,
            "owner": {
                "id": 0,
                "login": "Mergifyio",
                "type": "User",
            },
        },
    }
