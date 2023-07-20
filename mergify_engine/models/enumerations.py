from __future__ import annotations

import enum


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


class GithubAuthenticatedActorType(str, enum.Enum):
    USER = "user"
    APPLICATION = "application"
