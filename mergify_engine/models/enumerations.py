from __future__ import annotations

import enum


class EventType(enum.StrEnum):
    ActionAssign = "action.assign"
    ActionBackport = "action.backport"
    ActionClose = "action.close"
    ActionComment = "action.comment"
    ActionCopy = "action.copy"
    ActionDeleteHeadBranch = "action.delete_head_branch"
    ActionDismissReviews = "action.dismiss_reviews"
    ActionEdit = "action.edit"
    ActionLabel = "action.label"
    ActionMerge = "action.merge"
    ActionPostCheck = "action.post_check"
    ActionQueueEnter = "action.queue.enter"
    ActionQueueChecksStart = "action.queue.checks_start"
    ActionQueueChecksEnd = "action.queue.checks_end"
    ActionQueueLeave = "action.queue.leave"
    ActionQueueMerged = "action.queue.merged"
    ActionRebase = "action.rebase"
    ActionRefresh = "action.refresh"
    ActionRequestReviews = "action.request_reviews"
    ActionRequeue = "action.requeue"
    ActionReview = "action.review"
    ActionSquash = "action.squash"
    ActionUnqueue = "action.unqueue"
    ActionUpdate = "action.update"
    QueueFreezeCreate = "queue.freeze.create"
    QueueFreezeUpdate = "queue.freeze.update"
    QueueFreezeDelete = "queue.freeze.delete"
    QueuePauseCreate = "queue.pause.create"
    QueuePauseUpdate = "queue.pause.update"
    QueuePauseDelete = "queue.pause.delete"
    ActionGithubActions = "action.github_actions"
    ActionQueueChange = "action.queue.change"


class CheckConclusion(str, enum.Enum):
    # NOTE(lecrepont01): handles checks runs API conclusion values
    PENDING = None
    CANCELLED = "cancelled"
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"
    NEUTRAL = "neutral"
    STALE = "stale"
    ACTION_REQUIRED = "action_required"
    TIMED_OUT = "timed_out"


class CheckConclusionWithStatuses(str, enum.Enum):
    # NOTE(lecrepont01): also handles the commit statuses values
    SUCCESS = "success"
    FAILURE = "failure"
    NEUTRAL = "neutral"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"
    STALE = "stale"
    ACTION_REQUIRED = "action_required"
    TIMED_OUT = "timed_out"
    ERROR = "error"
    PENDING = "pending"


class ReviewType(str, enum.Enum):
    APPROVE = "APPROVE"
    REQUEST_CHANGES = "REQUEST_CHANGES"
    COMMENT = "COMMENT"


class QueueChecksAbortCode(str, enum.Enum):
    PR_DEQUEUED = "PR_DEQUEUED"
    PR_AHEAD_DEQUEUED = "PR_AHEAD_DEQUEUED"
    PR_AHEAD_FAILED_TO_MERGE = "PR_AHEAD_FAILED_TO_MERGE"
    PR_WITH_HIGHER_PRIORITY_QUEUED = "PR_WITH_HIGHER_PRIORITY_QUEUED"
    PR_QUEUED_TWICE = "PR_QUEUED_TWICE"
    SPECULATIVE_CHECK_NUMBER_REDUCED = "SPECULATIVE_CHECK_NUMBER_REDUCED"
    CHECKS_TIMEOUT = "CHECKS_TIMEOUT"
    CHECKS_FAILED = "CHECKS_FAILED"
    QUEUE_RULE_MISSING = "QUEUE_RULE_MISSING"
    UNEXPECTED_QUEUE_CHANGE = "UNEXPECTED_QUEUE_CHANGE"
    PR_FROZEN_NO_CASCADING = "PR_FROZEN_NO_CASCADING"
    BASE_BRANCH_MISSING = "BASE_BRANCH_MISSING"
    BASE_BRANCH_CHANGED = "BASE_BRANCH_CHANGED"
    PR_UNEXPECTEDLY_FAILED_TO_MERGE = "PR_UNEXPECTEDLY_FAILED_TO_MERGE"
    BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS = "BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS"
    PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE = (
        "PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE"
    )
    CONFLICT_WITH_BASE_BRANCH = "CONFLICT_WITH_BASE_BRANCH"
    CONFLICT_WITH_PULL_AHEAD = "CONFLICT_WITH_PULL_AHEAD"
    BRANCH_UPDATE_FAILED = "BRANCH_UPDATE_FAILED"
    DRAFT_PULL_REQUEST_CHANGED = "DRAFT_PULL_REQUEST_CHANGED"
    PULL_REQUEST_UPDATED = "PULL_REQUEST_UPDATED"
    MERGE_QUEUE_RESET = "MERGE_QUEUE_RESET"


class QueueChecksAbortStatus(str, enum.Enum):
    DEFINITIVE = "DEFINITIVE"
    REEMBARKED = "REEMBARKED"
