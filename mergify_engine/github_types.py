import dataclasses
import enum
import functools
import typing


ISODateTimeType = typing.NewType("ISODateTimeType", str)

GitHubLogin = typing.NewType("GitHubLogin", str)
GitHubLoginUnknown = typing.NewType("GitHubLoginUnknown", str)
GitHubLoginForTracing = typing.Union[GitHubLogin, GitHubLoginUnknown]


class GitHubInstallationAccessToken(typing.TypedDict):
    # https://developer.github.com/v3/apps/#response-7
    token: str
    expires_at: str


GitHubAccountType = typing.Literal["User", "Organization", "Bot"]
GitHubAccountIdType = typing.NewType("GitHubAccountIdType", int)


class GitHubAccount(typing.TypedDict):
    login: GitHubLogin
    id: GitHubAccountIdType
    type: GitHubAccountType
    avatar_url: str


GitHubInstallationIdType = typing.NewType("GitHubInstallationIdType", int)

GitHubInstallationPermissionsK = typing.Literal[
    "checks",
    "contents",
    "issues",
    "metadata",
    "pages",
    "pull_requests",
    "statuses",
    "members",
    "workflows",
]


GitHubInstallationPermissionsV = typing.Literal[
    "read",
    "write",
]

GitHubInstallationPermissions = typing.Dict[
    GitHubInstallationPermissionsK, GitHubInstallationPermissionsV
]


class GitHubInstallation(typing.TypedDict):
    # https://developer.github.com/v3/apps/#get-an-organization-installation-for-the-authenticated-app
    id: GitHubInstallationIdType
    account: GitHubAccount
    target_type: GitHubAccountType
    permissions: GitHubInstallationPermissions


GitHubRefType = typing.NewType("GitHubRefType", str)
SHAType = typing.NewType("SHAType", str)
GitHubRepositoryIdType = typing.NewType("GitHubRepositoryIdType", int)


GitHubRepositoryName = typing.NewType("GitHubRepositoryName", str)
GitHubRepositoryNameUnknown = typing.NewType("GitHubRepositoryNameUnknown", str)
GitHubRepositoryNameForTracing = typing.Union[
    GitHubRepositoryName, GitHubRepositoryNameUnknown
]
GitHubFilePath = typing.NewType("GitHubFilePath", str)


class GitHubRepository(typing.TypedDict):
    id: GitHubRepositoryIdType
    owner: GitHubAccount
    private: bool
    name: GitHubRepositoryName
    full_name: str
    archived: bool
    url: str
    html_url: str
    default_branch: GitHubRefType


@functools.total_ordering
class GitHubRepositoryPermission(enum.Enum):
    level: int
    _ignore_ = "level"

    def __new__(cls, permission: str) -> "GitHubRepositoryPermission":
        member = object.__new__(cls)
        member._value_ = permission
        member.level = len(cls.__members__)
        return member

    _order_ = "NONE READ WRITE ADMIN"
    NONE = "none"
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"

    @classmethod
    def _missing_(cls, value: object) -> None:
        allowed_permissions_str = ", ".join(map(str.lower, cls.__members__.keys()))
        raise ValueError(f"Permission must be one of ({allowed_permissions_str})")

    @classmethod
    def default(cls) -> "GitHubRepositoryPermission":
        return cls.NONE

    @classmethod
    def permissions_above(
        cls, permission: "GitHubRepositoryPermission"
    ) -> list["GitHubRepositoryPermission"]:
        """Return all permissions including the permission and above it"""
        return [p for p in cls.__members__.values() if p >= permission]

    def __lt__(self, other: "GitHubRepositoryPermission") -> bool:
        if isinstance(other, GitHubRepositoryPermission):
            return self.level < other.level
        return NotImplemented


class GitHubRepositoryCollaboratorPermission(typing.TypedDict):
    permission: GitHubRepositoryPermission
    user: GitHubAccount


GitHubTeamSlug = typing.NewType("GitHubTeamSlug", str)


class GitHubTeam(typing.TypedDict):
    slug: GitHubTeamSlug


class GitHubBranchCommitParent(typing.TypedDict):
    sha: SHAType


class GitHubBranchCommitVerification(typing.TypedDict):
    verified: bool


class GitHubAuthorCommitterCommit(typing.TypedDict):
    name: str
    date: ISODateTimeType
    email: str


class GitHubBranchCommitCommit(typing.TypedDict):
    message: str
    verification: GitHubBranchCommitVerification
    author: GitHubAuthorCommitterCommit
    committer: GitHubAuthorCommitterCommit


class GitHubBranchCommit(typing.TypedDict):
    sha: SHAType
    parents: typing.List[GitHubBranchCommitParent]
    commit: GitHubBranchCommitCommit
    committer: GitHubAccount


@dataclasses.dataclass
class CachedGitHubBranchCommit:
    sha: SHAType
    parents: typing.List[SHAType]
    commit_message: str
    commit_verification_verified: bool
    author: str
    committer: str
    date_author: ISODateTimeType
    date_committer: ISODateTimeType
    email_author: str
    email_committer: str
    __string_like__ = True

    def __str__(self) -> str:
        return self.commit_message


def to_cached_github_branch_commit(
    commit: GitHubBranchCommit,
) -> CachedGitHubBranchCommit:
    return CachedGitHubBranchCommit(
        sha=commit["sha"],
        commit_message=commit["commit"]["message"],
        commit_verification_verified=commit["commit"]["verification"]["verified"],
        parents=[p["sha"] for p in commit["parents"]],
        author=commit["commit"]["author"]["name"],
        committer=commit["commit"]["committer"]["name"],
        email_author=commit["commit"]["author"]["email"],
        email_committer=commit["commit"]["committer"]["email"],
        date_author=commit["commit"]["author"]["date"],
        date_committer=commit["commit"]["committer"]["date"],
    )


class GitHubBranchProtectionRequiredStatusChecks(typing.TypedDict):
    contexts: typing.List[str]
    strict: bool


class GitHubBranchProtectionRequirePullRequestReviews(typing.TypedDict):
    require_code_owner_reviews: bool
    required_approving_review_count: int


class GitHubBranchProtectionBoolean(typing.TypedDict):
    enabled: bool


class GitHubBranchProtection(typing.TypedDict, total=False):
    required_linear_history: GitHubBranchProtectionBoolean
    required_status_checks: GitHubBranchProtectionRequiredStatusChecks
    required_pull_request_reviews: GitHubBranchProtectionRequirePullRequestReviews
    required_conversation_resolution: GitHubBranchProtectionBoolean


class GitHubBranchProtectionLight(typing.TypedDict):
    enabled: bool
    required_status_checks: GitHubBranchProtectionRequiredStatusChecks


class GitHubBranch(typing.TypedDict):
    name: GitHubRefType
    commit: GitHubBranchCommit
    protection: GitHubBranchProtectionLight
    protected: bool


GitHubBaseBranchLabel = typing.NewType("GitHubBaseBranchLabel", str)


class GitHubBaseBranchRef(typing.TypedDict):
    label: GitHubBaseBranchLabel
    ref: GitHubRefType
    sha: SHAType
    repo: GitHubRepository
    user: GitHubAccount


GitHubHeadBranchLabel = typing.NewType("GitHubHeadBranchLabel", str)


class GitHubHeadBranchRef(typing.TypedDict):
    label: GitHubHeadBranchLabel
    ref: GitHubRefType
    sha: SHAType
    repo: typing.Optional[GitHubRepository]
    user: GitHubAccount


class GitHubLabel(typing.TypedDict):
    id: int
    name: str
    color: str
    default: bool


GitHubCommentIdType = typing.NewType("GitHubCommentIdType", int)


class GitHubComment(typing.TypedDict):
    id: GitHubCommentIdType
    url: str
    body: str
    user: GitHubAccount
    created_at: ISODateTimeType
    updated_at: ISODateTimeType


GitHubCommentChangesBody = typing.TypedDict("GitHubCommentChangesBody", {"from": str})


class GitHubCommentChanges(typing.TypedDict):
    body: GitHubCommentChangesBody


class GitHubContentFile(typing.TypedDict):
    type: typing.Literal["file"]
    content: str
    sha: SHAType
    path: GitHubFilePath


class GitHubFile(typing.TypedDict):
    sha: SHAType
    filename: str
    contents_url: str
    status: typing.Union[typing.Literal["added"], typing.Literal["removed"]]
    additions: int
    deletions: int
    changes: int
    blob_url: str
    raw_url: str
    patch: str


class CachedGitHubFile(typing.TypedDict):
    filename: str


class GitHubIssueOrPullRequest(typing.TypedDict):
    pass


GitHubIssueId = typing.NewType("GitHubIssueId", int)
GitHubIssueNumber = typing.NewType("GitHubIssueNumber", int)


class GitHubIssue(GitHubIssueOrPullRequest):
    id: GitHubIssueId
    number: GitHubIssueNumber
    user: GitHubAccount


GitHubPullRequestState = typing.Literal["open", "closed"]

# NOTE(sileht): Github mergeable_state is undocumented, here my finding by
# testing and and some info from other project:
#
# unknown: not yet computed by Github
# dirty: pull request conflict with the base branch
# behind: head branch is behind the base branch (only if strict: True)
# unstable: branch up2date (if strict: True) and not required status
#           checks are failure or pending
# clean: branch up2date (if strict: True) and all status check OK
# has_hooks: Mergeable with passing commit status and pre-recieve hooks.
#
# https://platform.github.community/t/documentation-about-mergeable-state/4259
# https://github.com/octokit/octokit.net/issues/1763
# https://developer.github.com/v4/enum/mergestatestatus/

GitHubPullRequestMergeableState = typing.Literal[
    "unknown",
    "dirty",
    "behind",
    "unstable",
    "clean",
    "has_hooks",
]

GitHubPullRequestId = typing.NewType("GitHubPullRequestId", int)
GitHubPullRequestNumber = typing.NewType("GitHubPullRequestNumber", int)


class GitHubMilestone(typing.TypedDict):
    id: int
    number: int
    title: str


class GitHubPullRequest(GitHubIssueOrPullRequest):
    # https://docs.github.com/en/rest/reference/pulls#get-a-pull-request
    id: GitHubPullRequestId
    number: GitHubPullRequestNumber
    maintainer_can_modify: bool
    base: GitHubBaseBranchRef
    head: GitHubHeadBranchRef
    state: GitHubPullRequestState
    user: GitHubAccount
    labels: typing.List[GitHubLabel]
    merged: bool
    merged_by: typing.Optional[GitHubAccount]
    merged_at: typing.Optional[ISODateTimeType]
    rebaseable: bool
    draft: bool
    merge_commit_sha: typing.Optional[SHAType]
    mergeable: typing.Optional[bool]
    mergeable_state: GitHubPullRequestMergeableState
    html_url: str
    issue_url: str
    title: str
    body: typing.Optional[str]
    changed_files: int
    commits: int
    locked: bool
    assignees: typing.List[GitHubAccount]
    requested_reviewers: typing.List[GitHubAccount]
    requested_teams: typing.List[GitHubTeam]
    milestone: typing.Optional[GitHubMilestone]
    updated_at: ISODateTimeType
    created_at: ISODateTimeType
    closed_at: typing.Optional[ISODateTimeType]
    node_id: str


# https://docs.github.com/en/developers/webhooks-and-events/webhooks/webhook-events-and-payloads
GitHubEventType = typing.Literal[
    "check_run",
    "check_suite",
    "pull_request",
    "status",
    "push",
    "issue_comment",
    "pull_request_review",
    "pull_request_review_comment",
    "pull_request_review_thread",
    "repository",
    # This does not exist in GitHub, it's a Mergify made one
    "refresh",
]


class GitHubEvent(typing.TypedDict):
    organization: GitHubAccount
    installation: GitHubInstallation
    sender: GitHubAccount
    # NOTE(sileht):  Injected by Mergify webhook receiver
    received_at: ISODateTimeType


GitHubEventRefreshActionType = typing.Literal[
    "user",
    "internal",
    "admin",
]


# This does not exist in GitHub, it's a Mergify made one
class GitHubEventRefresh(GitHubEvent):
    repository: GitHubRepository
    action: GitHubEventRefreshActionType
    ref: typing.Optional[GitHubRefType]
    pull_request_number: typing.Optional[GitHubPullRequestNumber]
    source: str


GitHubEventPullRequestActionType = typing.Literal[
    "opened",
    "edited",
    "closed",
    "assigned",
    "unassigned",
    "review_requested",
    "review_request_removed",
    "ready_for_review",
    "labeled",
    "unlabeled",
    "synchronize",
    "locked",
    "unlocked",
    "reopened",
]


class GitHubEventPullRequest(GitHubEvent):
    repository: GitHubRepository
    action: GitHubEventPullRequestActionType
    pull_request: GitHubPullRequest
    number: GitHubPullRequestNumber
    # At least in action=synchronize
    after: SHAType
    before: SHAType


GitHubEventRepositoryActionType = typing.Literal[
    "created",
    "deleted",
    "archived",
    "unarchived",
    "edited",
    "renamed",
    "transferred",
    "publicized",
    "privatized",
]


class GitHubEventRepository(GitHubEvent):
    repository: GitHubRepository
    action: GitHubEventRepositoryActionType
    pull_request: GitHubPullRequest


GitHubEventPullRequestReviewCommentActionType = typing.Literal[
    "created",
    "edited",
    "deleted",
]


class GitHubEventPullRequestReviewComment(GitHubEvent):
    repository: GitHubRepository
    action: GitHubEventPullRequestReviewCommentActionType
    pull_request: typing.Optional[GitHubPullRequest]
    comment: typing.Optional[GitHubComment]


GitHubEventPullRequestReviewActionType = typing.Literal[
    "submitted",
    "edited",
    "dismissed",
]


GitHubReviewIdType = typing.NewType("GitHubReviewIdType", int)
GitHubReviewStateType = typing.Literal[
    "APPROVED", "COMMENTED", "DISMISSED", "CHANGES_REQUESTED"
]
GitHubReviewStateChangeType = typing.Literal["APPROVE", "REQUEST_CHANGES", "COMMENT"]


class GitHubReviewPostMandatory(typing.TypedDict):
    event: GitHubReviewStateChangeType


class GitHubReviewPost(GitHubReviewPostMandatory, total=False):
    body: str


# https://docs.github.com/en/graphql/reference/enums#commentauthorassociation
GitHubCommentAuthorAssociation = typing.Literal[
    "COLLABORATOR",
    "CONTRIBUTOR",
    "FIRST_TIMER",
    "FIRST_TIME_CONTRIBUTOR",
    "MANNEQUIN",
    "MEMBER",
    "NONE",
    "OWNER",
]


class GitHubReview(typing.TypedDict):
    id: GitHubReviewIdType
    user: GitHubAccount
    body: typing.Optional[str]
    pull_request: GitHubPullRequest
    repository: GitHubRepository
    state: GitHubReviewStateType
    author_association: GitHubCommentAuthorAssociation
    submitted_at: ISODateTimeType


class GitHubEventPullRequestReview(GitHubEvent):
    repository: GitHubRepository
    action: GitHubEventPullRequestReviewActionType
    pull_request: GitHubPullRequest
    review: GitHubReview


class GitHubEventPullRequestReviewThread(GitHubEventPullRequestReview):
    pass


GitHubEventIssueCommentActionType = typing.Literal[
    "created",
    "edited",
    "deleted",
]


class GitHubEventIssueComment(GitHubEvent):
    repository: GitHubRepository
    action: GitHubEventIssueCommentActionType
    issue: GitHubIssue
    comment: GitHubComment
    changes: GitHubCommentChanges


class GitHubEventPushCommit(typing.TypedDict):
    added: typing.List[str]
    modified: typing.List[str]
    removed: typing.List[str]


class GitHubEventPush(GitHubEvent):
    repository: GitHubRepository
    ref: GitHubRefType
    forced: bool
    before: SHAType
    after: SHAType
    commits: typing.List[GitHubEventPushCommit]


class GitHubEventStatus(GitHubEvent):
    repository: GitHubRepository
    sha: SHAType


class GitHubApp(typing.TypedDict):
    id: int
    name: str
    owner: GitHubAccount


GitHubCheckRunConclusion = typing.Literal[
    "success",
    "failure",
    "neutral",
    "cancelled",
    "skipped",
    "timed_out",
    "action_required",
    "stale",
    None,
]


class GitHubCheckRunOutput(typing.TypedDict):
    title: str
    summary: str
    text: typing.Optional[str]
    annotations: typing.Optional[typing.List[str]]
    annotations_count: int
    annotations_url: str


GitHubStatusState = typing.Literal[
    "pending",
    "success",
    "failure",
    "error",
]


class GitHubStatus(typing.TypedDict):
    context: str
    state: GitHubStatusState
    description: str
    target_url: str
    avatar_url: str


GitHubCheckRunStatus = typing.Literal["queued", "in_progress", "completed"]


class GitHubCheckRunCheckSuite(typing.TypedDict):
    id: int


class GitHubCheckRun(typing.TypedDict):
    id: int
    app: GitHubApp
    external_id: str
    pull_requests: typing.List[GitHubPullRequest]
    head_sha: SHAType
    before: SHAType
    after: SHAType
    name: str
    status: GitHubCheckRunStatus
    output: GitHubCheckRunOutput
    conclusion: typing.Optional[GitHubCheckRunConclusion]
    started_at: ISODateTimeType
    completed_at: ISODateTimeType | None
    html_url: str
    details_url: str
    check_suite: GitHubCheckRunCheckSuite


class CachedGitHubCheckRun(typing.TypedDict):
    id: int
    app_id: int
    app_name: str
    app_avatar_url: str
    external_id: str
    head_sha: SHAType
    name: str
    status: GitHubCheckRunStatus
    output: GitHubCheckRunOutput
    conclusion: typing.Optional[GitHubCheckRunConclusion]
    completed_at: ISODateTimeType | None
    html_url: str


class GitHubCheckSuite(typing.TypedDict):
    id: int
    app: GitHubApp
    external_id: str
    pull_requests: typing.List[GitHubPullRequest]
    head_sha: SHAType
    before: SHAType
    after: SHAType


GitHubCheckRunActionType = typing.Literal[
    "created",
    "completed",
    "rerequested",
    "requested_action",
]


class GitHubEventCheckRun(GitHubEvent):
    repository: GitHubRepository
    action: GitHubCheckRunActionType
    app: GitHubApp
    check_run: GitHubCheckRun


GitHubCheckSuiteActionType = typing.Literal[
    "created",
    "completed",
    "rerequested",
    "requested_action",
]


class GitHubEventCheckSuite(GitHubEvent):
    repository: GitHubRepository
    action: GitHubCheckSuiteActionType
    app: GitHubApp
    check_suite: GitHubCheckSuite


GitHubEventOrganizationActionType = typing.Literal[
    "deleted",
    "renamed",
    "member_added",
    "member_removed",
    "member_invited",
]


class GitHubEventOrganization(GitHubEvent):
    action: GitHubEventOrganizationActionType


GitHubEventMemberActionType = typing.Literal["added", "removed", "edited"]


class GitHubEventMember(GitHubEvent):
    action: GitHubEventMemberActionType
    repository: GitHubRepository


GitHubEventMembershipActionType = typing.Literal["added", "removed"]


class GitHubEventMembership(GitHubEvent):
    action: GitHubEventMembershipActionType
    team: GitHubTeam


GitHubEventTeamActionType = typing.Literal[
    "created",
    "deleted",
    "edited",
    "added_to_repository",
    "removed_from_repository",
]


class GitHubEventTeam(GitHubEvent):
    action: GitHubEventTeamActionType
    repository: typing.Optional[GitHubRepository]
    team: GitHubTeam


class GitHubEventTeamAdd(GitHubEvent, total=False):
    # Repository key can be missing on Enterprise installations
    repository: GitHubRepository


GitHubGitRefType = typing.NewType("GitHubGitRefType", str)


class GitHubGitRef(typing.TypedDict):
    ref: GitHubRefType


class GitHubRequestedReviewers(typing.TypedDict):
    users: typing.List[GitHubAccount]
    teams: typing.List[GitHubTeam]


GitHubApiVersion = typing.Literal[
    "squirrel-girl", "lydian", "groot", "antiope", "luke-cage"
]
GitHubOAuthToken = typing.NewType("GitHubOAuthToken", str)


GitHubAnnotationLevel = typing.Literal["failure"]


class GitHubAnnotation(typing.TypedDict):
    path: str
    start_line: int
    end_line: int
    start_column: int
    end_column: int
    annotation_level: GitHubAnnotationLevel
    message: str
    title: str


class GitHubCompareCommits(typing.TypedDict):
    behind_by: int
