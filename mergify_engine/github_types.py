import dataclasses
import enum
import functools
import typing

import typing_extensions


ISODateTimeType = typing.NewType("ISODateTimeType", str)

GitHubLogin = typing.NewType("GitHubLogin", str)
GitHubLoginUnknown = typing.NewType("GitHubLoginUnknown", str)
GitHubLoginForTracing = GitHubLogin | GitHubLoginUnknown


class GitHubInstallationAccessToken(typing_extensions.TypedDict):
    # https://developer.github.com/v3/apps/#response-7
    token: str
    expires_at: str


GitHubAccountType = typing.Literal["User", "Organization", "Bot"]
GitHubAccountIdType = typing.NewType("GitHubAccountIdType", int)


class GitHubAccount(typing_extensions.TypedDict):
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
    "actions",
]


GitHubInstallationPermissionsV = typing.Literal[
    "read",
    "write",
]

GitHubInstallationPermissions = dict[
    GitHubInstallationPermissionsK,
    GitHubInstallationPermissionsV,
]


class GitHubInstallation(typing_extensions.TypedDict):
    # https://developer.github.com/v3/apps/#get-an-organization-installation-for-the-authenticated-app
    id: GitHubInstallationIdType
    account: GitHubAccount
    target_type: GitHubAccountType
    permissions: GitHubInstallationPermissions
    suspended_at: ISODateTimeType | None
    app_slug: typing.NotRequired[str]


GitHubRefType = typing.NewType("GitHubRefType", str)
SHAType = typing.NewType("SHAType", str)
GitHubRepositoryIdType = typing.NewType("GitHubRepositoryIdType", int)


GitHubRepositoryName = typing.NewType("GitHubRepositoryName", str)
GitHubRepositoryNameUnknown = typing.NewType("GitHubRepositoryNameUnknown", str)
GitHubRepositoryNameForTracing = GitHubRepositoryName | GitHubRepositoryNameUnknown
GitHubFilePath = typing.NewType("GitHubFilePath", str)


class GitHubRepository(typing_extensions.TypedDict):
    id: GitHubRepositoryIdType
    owner: GitHubAccount
    private: bool
    name: GitHubRepositoryName
    full_name: str
    archived: bool
    url: str
    html_url: str
    default_branch: GitHubRefType


class GitHubRepositoryList(typing_extensions.TypedDict):
    repositories: list[GitHubRepository]
    total_count: int


GitHubRepositoryPermissionLiteral = typing.Literal[
    "none",
    "read",
    "write",
    "maintain",
    "admin",
]


@functools.total_ordering
class GitHubRepositoryPermission(enum.Enum):
    level: int
    _ignore_ = "level"

    def __new__(cls, permission: str) -> "GitHubRepositoryPermission":
        member = object.__new__(cls)
        member._value_ = permission
        member.level = len(cls.__members__)
        return member

    _order_ = "NONE READ WRITE MAINTAIN ADMIN"
    NONE = "none"
    READ = "read"
    WRITE = "write"
    MAINTAIN = "maintain"
    ADMIN = "admin"

    @classmethod
    def _missing_(cls, _value: object) -> None:
        allowed_permissions_str = ", ".join(map(str.lower, cls.__members__.keys()))
        raise ValueError(f"Permission must be one of ({allowed_permissions_str})")

    @classmethod
    def default(cls) -> "GitHubRepositoryPermission":
        return cls.NONE

    @classmethod
    def permissions_above(
        cls,
        permission: "GitHubRepositoryPermission",
    ) -> list["GitHubRepositoryPermission"]:
        """Return all permissions including the permission and above it"""
        return [p for p in cls.__members__.values() if p >= permission]

    def __lt__(self, other: typing.Any) -> bool:
        if isinstance(other, GitHubRepositoryPermission):
            return self.level < other.level
        return NotImplemented


class GitHubRepositoryCollaboratorPermission(typing_extensions.TypedDict):
    permission: GitHubRepositoryPermission
    user: GitHubAccount


GitHubTeamSlug = typing.NewType("GitHubTeamSlug", str)


class GitHubTeam(typing_extensions.TypedDict):
    slug: GitHubTeamSlug


class GitHubBranchCommitParent(typing_extensions.TypedDict):
    sha: SHAType


class GitHubBranchCommitVerification(typing_extensions.TypedDict):
    verified: bool


class GitHubAuthorCommitterCommit(typing_extensions.TypedDict):
    name: str
    date: ISODateTimeType
    email: str


class GitHubAuthorCommitterCommitWithUsername(GitHubAuthorCommitterCommit):
    username: str


class GitHubBranchCommitCommit(typing_extensions.TypedDict):
    message: str
    verification: GitHubBranchCommitVerification
    author: GitHubAuthorCommitterCommit | None
    committer: GitHubAuthorCommitterCommit | None


class GitHubBranchCommit(typing_extensions.TypedDict):
    sha: SHAType
    parents: list[GitHubBranchCommitParent]
    commit: GitHubBranchCommitCommit
    committer: GitHubAccount | None
    author: GitHubAccount | None


@dataclasses.dataclass
class CachedGitHubBranchCommit:
    sha: SHAType
    parents: list[SHAType]
    commit_message: str
    commit_verification_verified: bool
    author: str | None
    committer: str | None
    date_author: ISODateTimeType | None
    date_committer: ISODateTimeType | None
    email_author: str | None
    email_committer: str | None
    gh_author_login: GitHubLogin | None

    __string_like__ = True

    def __str__(self) -> str:
        return self.commit_message


def to_cached_github_branch_commit(
    commit: GitHubBranchCommit,
) -> CachedGitHubBranchCommit:
    author = commit["author"]
    gh_author_login = None if author is None else author.get("login")

    if commit["commit"]["author"] is None:
        commit_author_name = None
        commit_author_email = None
        commit_author_date = None
    else:
        commit_author_name = commit["commit"]["author"]["name"]
        commit_author_email = commit["commit"]["author"]["email"]
        commit_author_date = commit["commit"]["author"]["date"]

    if commit["commit"]["committer"] is None:
        commit_committer_name = None
        commit_committer_email = None
        commit_committer_date = None
    else:
        commit_committer_name = commit["commit"]["committer"]["name"]
        commit_committer_email = commit["commit"]["committer"]["email"]
        commit_committer_date = commit["commit"]["committer"]["date"]

    return CachedGitHubBranchCommit(
        sha=commit["sha"],
        commit_message=commit["commit"]["message"],
        commit_verification_verified=commit["commit"]["verification"]["verified"],
        parents=[p["sha"] for p in commit["parents"]],
        author=commit_author_name,
        committer=commit_committer_name,
        email_author=commit_author_email,
        email_committer=commit_committer_email,
        date_author=commit_author_date,
        date_committer=commit_committer_date,
        gh_author_login=gh_author_login,
    )


class GitHubBranchProtectionRequiredStatusChecks(typing_extensions.TypedDict):
    contexts: list[str]
    strict: bool


class GitHubBranchProtectionRequirePullRequestReviews(typing_extensions.TypedDict):
    require_code_owner_reviews: bool
    required_approving_review_count: int


class GitHubBranchProtectionBoolean(typing_extensions.TypedDict):
    enabled: bool


class GitHubBranchProtection(typing_extensions.TypedDict, total=False):
    required_linear_history: GitHubBranchProtectionBoolean
    required_status_checks: GitHubBranchProtectionRequiredStatusChecks
    required_pull_request_reviews: GitHubBranchProtectionRequirePullRequestReviews
    required_conversation_resolution: GitHubBranchProtectionBoolean


class GitHubBranchProtectionLight(typing_extensions.TypedDict):
    enabled: bool
    required_status_checks: GitHubBranchProtectionRequiredStatusChecks


class GitHubBranch(typing_extensions.TypedDict):
    name: GitHubRefType
    commit: GitHubBranchCommit
    protection: GitHubBranchProtectionLight
    protected: bool


GitHubBaseBranchLabel = typing.NewType("GitHubBaseBranchLabel", str)


class GitHubBaseBranchRef(typing_extensions.TypedDict):
    label: GitHubBaseBranchLabel
    ref: GitHubRefType
    sha: SHAType
    repo: GitHubRepository
    user: GitHubAccount


GitHubHeadBranchLabel = typing.NewType("GitHubHeadBranchLabel", str)


class GitHubHeadBranchRef(typing_extensions.TypedDict):
    label: GitHubHeadBranchLabel
    ref: GitHubRefType
    sha: SHAType
    repo: GitHubRepository | None
    user: GitHubAccount


class GitHubLabel(typing_extensions.TypedDict):
    id: int
    name: str
    color: str
    default: bool


GitHubCommentIdType = typing.NewType("GitHubCommentIdType", int)


class GitHubComment(typing_extensions.TypedDict):
    id: GitHubCommentIdType
    url: str
    body: str
    user: GitHubAccount
    created_at: ISODateTimeType
    updated_at: ISODateTimeType


GitHubCommentChangesBody = typing_extensions.TypedDict(
    "GitHubCommentChangesBody",
    {"from": str},
)


class GitHubCommentChanges(typing_extensions.TypedDict):
    body: GitHubCommentChangesBody


class GitHubContentFile(typing_extensions.TypedDict):
    type: typing.Literal["file"]
    content: str
    sha: SHAType
    path: GitHubFilePath
    encoding: typing.Literal["base64", "none"]


GitHubFileStatus = typing.Literal[
    "added",
    "removed",
    "modified",
    "renamed",
    "copied",
    "changed",
    "unchanged",
]


class GitHubFile(typing_extensions.TypedDict):
    sha: SHAType
    filename: str
    contents_url: str
    status: GitHubFileStatus
    additions: int
    deletions: int
    changes: int
    blob_url: str
    raw_url: str
    patch: str
    previous_filename: str | None


class CachedGitHubFile(typing_extensions.TypedDict):
    sha: SHAType
    filename: str
    contents_url: str
    status: GitHubFileStatus
    previous_filename: str | None


class GitHubIssueOrPullRequest(typing_extensions.TypedDict):
    pass


GitHubIssueId = typing.NewType("GitHubIssueId", int)
GitHubIssueNumber = typing.NewType("GitHubIssueNumber", int)


class GitHubIssue(GitHubIssueOrPullRequest):
    id: GitHubIssueId
    number: GitHubIssueNumber
    user: GitHubAccount


GitHubPullRequestState = typing.Literal["open", "closed"]

# NOTE(sileht): GitHub mergeable_state is undocumented, here my finding by
# testing and and some info from other project:
#
# unknown: not yet computed by GitHub
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
    "blocked",
]

GitHubPullRequestId = typing.NewType("GitHubPullRequestId", int)
GitHubPullRequestNumber = typing.NewType("GitHubPullRequestNumber", int)


class GitHubMilestone(typing_extensions.TypedDict):
    id: int
    number: int
    title: str


class GitHubPullRequestBase(GitHubIssueOrPullRequest):
    # The GitHub pull requests returned from `/pulls` do not have the same
    # properties as the one from `/pulls/{pr_number}`
    # https://docs.github.com/en/rest/pulls/pulls?apiVersion=2022-11-28#list-pull-requests
    id: GitHubPullRequestId
    number: GitHubPullRequestNumber
    base: GitHubBaseBranchRef
    head: GitHubHeadBranchRef
    state: GitHubPullRequestState
    user: GitHubAccount
    labels: list[GitHubLabel]
    merged_at: ISODateTimeType | None
    draft: bool
    merge_commit_sha: SHAType | None
    html_url: str
    issue_url: str
    title: str
    body: str | None
    locked: bool
    assignees: list[GitHubAccount]
    requested_reviewers: list[GitHubAccount]
    requested_teams: list[GitHubTeam]
    milestone: GitHubMilestone | None
    updated_at: ISODateTimeType
    created_at: ISODateTimeType
    closed_at: ISODateTimeType | None
    node_id: str


class GitHubPullRequest(GitHubPullRequestBase):
    # https://docs.github.com/en/rest/reference/pulls#get-a-pull-request
    maintainer_can_modify: bool
    merged: bool
    merged_by: GitHubAccount | None
    rebaseable: bool | None
    mergeable: bool | None
    mergeable_state: GitHubPullRequestMergeableState | None
    changed_files: int
    commits: int


# https://docs.github.com/en/developers/webhooks-and-events/webhooks/webhook-events-and-payloads
GitHubEventType = typing.Literal[
    "check_run",
    "check_suite",
    "pull_request",
    "status",
    "push",
    "issue_comment",
    "installation",
    "installation_repositories",
    "pull_request_review",
    "pull_request_review_comment",
    "pull_request_review_thread",
    "repository",
    "organization",
    "member",
    "membership",
    "team",
    "team_add",
    "workflow_job",
    "workflow_run",
    # This does not exist in GitHub, it's a Mergify made one
    "refresh",
]

# https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
GitHubWorkflowTriggerEventType = typing.Literal[
    "pull_request",
    "pull_request_target",
    "push",
    "schedule",
]


class GitHubEvent(typing_extensions.TypedDict):
    # FIXME(sileht): not all events have organization keys
    organization: GitHubAccount
    # FIXME(sileht): not all events have full installation object (sometimes
    # only the id is present)
    installation: GitHubInstallation
    sender: GitHubAccount
    # NOTE(sileht):  Injected by Mergify webhook receiver
    received_at: ISODateTimeType


class GitHubEventWithRepository(GitHubEvent):
    repository: GitHubRepository


class GitHubInstallationRepository(typing_extensions.TypedDict):
    full_name: str
    id: GitHubRepositoryIdType
    name: GitHubRepositoryName
    node_id: str
    private: bool


class GitHubEventInstallationRepositories(GitHubEventWithRepository):
    # https://docs.github.com/en/webhooks-and-events/webhooks/webhook-events-and-payloads#installation_repositories
    action: typing.Literal["added", "removed"]
    repositories_added: list[GitHubInstallationRepository]
    repositories_removed: list[GitHubInstallationRepository]
    repository_selection: typing.Literal["all", "selected"]
    requester: GitHubAccount


GitHubEventRefreshActionType = typing.Literal[
    "user",
    "internal",
    "admin",
]


# This does not exist in GitHub, it's a Mergify made one
class GitHubEventRefresh(GitHubEventWithRepository):
    action: GitHubEventRefreshActionType
    ref: GitHubRefType | None
    pull_request_number: GitHubPullRequestNumber | None
    source: str
    flag: str | None
    attempts: int | None


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
    "converted_to_draft",
]


class GitHubEventPullRequest(GitHubEventWithRepository):
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


class GitHubEventRepository(GitHubEventWithRepository):
    action: GitHubEventRepositoryActionType
    pull_request: GitHubPullRequest


GitHubEventPullRequestReviewCommentActionType = typing.Literal[
    "created",
    "edited",
    "deleted",
]


class GitHubEventPullRequestReviewComment(GitHubEventWithRepository):
    action: GitHubEventPullRequestReviewCommentActionType
    pull_request: GitHubPullRequest | None
    comment: GitHubComment | None


GitHubEventPullRequestReviewActionType = typing.Literal[
    "submitted",
    "edited",
    "dismissed",
]


GitHubReviewIdType = typing.NewType("GitHubReviewIdType", int)
GitHubReviewStateType = typing.Literal[
    "APPROVED",
    "COMMENTED",
    "DISMISSED",
    "CHANGES_REQUESTED",
]
GitHubEventReviewStateType = typing.Literal[
    "approved",
    "commented",
    "dismissed",
    "changes_requested",
]
GitHubReviewStateChangeType = typing.Literal["APPROVE", "REQUEST_CHANGES", "COMMENT"]


class GitHubReviewPostMandatory(typing_extensions.TypedDict):
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


class GitHubReview(typing_extensions.TypedDict):
    id: GitHubReviewIdType
    user: GitHubAccount | None
    body: str | None
    pull_request: GitHubPullRequest
    repository: GitHubRepository
    state: GitHubReviewStateType
    author_association: GitHubCommentAuthorAssociation
    submitted_at: ISODateTimeType


class GitHubEventPullRequestReview(GitHubEventWithRepository):
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


class GitHubEventIssueComment(GitHubEventWithRepository):
    action: GitHubEventIssueCommentActionType
    issue: GitHubIssue
    comment: GitHubComment
    changes: GitHubCommentChanges


class GitHubEventPushCommit(typing_extensions.TypedDict):
    added: list[str]
    modified: list[str]
    removed: list[str]
    message: str
    author: GitHubAuthorCommitterCommitWithUsername


class GitHubEventPush(GitHubEventWithRepository):
    ref: GitHubRefType
    forced: bool
    before: SHAType
    after: SHAType
    commits: list[GitHubEventPushCommit]
    head_commit: GitHubEventPushCommit


class GitHubEventStatus(GitHubEventWithRepository):
    sha: SHAType
    state: typing.Literal["pending", "success", "failure", "error"]
    name: str
    context: str
    target_url: str


class GitHubApp(typing_extensions.TypedDict):
    id: int
    name: str
    slug: str
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


class GitHubCheckRunOutput(typing_extensions.TypedDict):
    title: str | None
    summary: str | None
    text: str | None
    annotations_count: int
    annotations_url: str


GitHubStatusState = typing.Literal[
    "pending",
    "success",
    "failure",
    "error",
]


class GitHubStatus(typing_extensions.TypedDict):
    context: str
    state: GitHubStatusState
    description: str
    target_url: str
    avatar_url: str


GitHubCheckRunStatus = typing.Literal["pending", "queued", "in_progress", "completed"]


class GitHubCheckRunCheckSuite(typing_extensions.TypedDict):
    id: int


class GitHubCheckRunPullRequestRepository(typing_extensions.TypedDict):
    id: GitHubRepositoryIdType
    name: GitHubRepositoryName
    url: str


class GitHubCheckRunPullRequestHeadOrBaseBranch(typing_extensions.TypedDict):
    ref: GitHubRefType
    sha: SHAType
    repo: GitHubCheckRunPullRequestRepository


class GitHubCheckRunPullRequest(GitHubIssueOrPullRequest):
    id: GitHubPullRequestId
    number: GitHubPullRequestNumber
    base: GitHubCheckRunPullRequestHeadOrBaseBranch
    head: GitHubCheckRunPullRequestHeadOrBaseBranch
    url: str


class GitHubCheckRun(typing_extensions.TypedDict):
    id: int
    app: GitHubApp
    external_id: str
    pull_requests: list[GitHubCheckRunPullRequest]
    head_sha: SHAType
    name: str
    status: GitHubCheckRunStatus
    output: GitHubCheckRunOutput
    conclusion: GitHubCheckRunConclusion | None
    started_at: ISODateTimeType
    completed_at: ISODateTimeType | None
    html_url: str
    details_url: str
    check_suite: GitHubCheckRunCheckSuite


class GitHubCheckRunWithRepository(GitHubCheckRun):
    repository: GitHubRepository


class CachedGitHubCheckRun(typing_extensions.TypedDict):
    id: int
    app_id: int
    app_name: str
    app_avatar_url: str
    app_slug: str
    external_id: str
    head_sha: SHAType
    name: str
    status: GitHubCheckRunStatus
    output: GitHubCheckRunOutput
    conclusion: GitHubCheckRunConclusion | None
    completed_at: ISODateTimeType | None
    html_url: str


class GitHubCheckSuite(typing_extensions.TypedDict):
    id: int
    app: GitHubApp
    external_id: str
    pull_requests: list[GitHubPullRequest]
    head_sha: SHAType
    before: SHAType
    after: SHAType


GitHubCheckRunActionType = typing.Literal[
    "created",
    "completed",
    "rerequested",
    "requested_action",
]


class GitHubEventCheckRun(GitHubEventWithRepository):
    action: GitHubCheckRunActionType
    app: GitHubApp
    check_run: GitHubCheckRun


GitHubCheckSuiteActionType = typing.Literal[
    "created",
    "completed",
    "rerequested",
    "requested_action",
]


class GitHubEventCheckSuite(GitHubEventWithRepository):
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
    member: GitHubAccount


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
    repository: GitHubRepository | None
    team: GitHubTeam


class GitHubEventTeamAdd(GitHubEvent, total=False):
    # Repository key can be missing on Enterprise installations
    repository: GitHubRepository


GitHubGitRefType = typing.NewType("GitHubGitRefType", str)


class GitHubGitRef(typing_extensions.TypedDict):
    ref: GitHubRefType


class GitHubRequestedReviewers(typing_extensions.TypedDict):
    users: list[GitHubAccount]
    teams: list[GitHubTeam]


GitHubApiVersion = typing.Literal[
    "squirrel-girl",
    "lydian",
    "groot",
    "antiope",
    "luke-cage",
]
GitHubOAuthToken = typing.NewType("GitHubOAuthToken", str)


class GitHubUserToServerAuthorization(typing_extensions.TypedDict):
    access_token: GitHubOAuthToken
    expires_in: int
    refresh_token: str
    refresh_token_expires_in: int
    scope: typing.Literal[""]
    token_type: typing.Literal["bearer"]


GitHubAnnotationLevel = typing.Literal["failure"]


class GitHubAnnotation(typing_extensions.TypedDict):
    path: str
    start_line: int
    end_line: int
    start_column: int
    end_column: int
    annotation_level: GitHubAnnotationLevel
    message: str
    title: str


class GitHubCompareCommits(typing_extensions.TypedDict):
    behind_by: int
    status: typing.Literal["diverged", "ahead", "behind", "identical"]


GitHubMembershipRole = typing.Literal["admin", "member"]

GitHubOrganizationIdType = typing.NewType("GitHubOrganizationIdType", int)


class GitHubOrganization(typing_extensions.TypedDict):
    login: GitHubLogin
    id: GitHubOrganizationIdType


# https://docs.github.com/en/rest/orgs/members?apiVersion=2022-11-28#get-an-organization-membership-for-the-authenticated-user
class GitHubMembership(typing_extensions.TypedDict):
    state: typing.Literal["active", "pending"]
    role: GitHubMembershipRole
    user: GitHubAccount
    organization: GitHubOrganization


GitHubWorkflowRunConclusionType = typing.Literal[
    "success",
    "failure",
    "skipped",
    "cancelled",
    None,
]


class GitHubWorkflowRunPullRequestRepository(typing_extensions.TypedDict):
    id: GitHubRepositoryIdType
    name: GitHubRepositoryName
    url: str


class GitHubWorkflowRunPullRequestBaseOrHead(typing_extensions.TypedDict):
    ref: GitHubRefType
    sha: SHAType
    repo: GitHubWorkflowRunPullRequestRepository


class GitHubWorkflowRunPullRequest(typing_extensions.TypedDict):
    id: GitHubPullRequestId
    number: GitHubPullRequestNumber
    base: GitHubWorkflowRunPullRequestBaseOrHead
    head: GitHubWorkflowRunPullRequestBaseOrHead
    url: str


class GitHubWorkflowRun(typing_extensions.TypedDict):
    id: int
    workflow_id: int
    name: str
    event: GitHubWorkflowTriggerEventType
    conclusion: GitHubWorkflowRunConclusionType
    triggering_actor: GitHubAccount
    jobs_url: str
    head_sha: SHAType
    repository: GitHubRepository
    run_attempt: int
    run_started_at: ISODateTimeType
    pull_requests: list[GitHubWorkflowRunPullRequest]


class GitHubEventWorkflowRun(GitHubEventWithRepository):
    action: GitHubEventPullRequestActionType
    workflow_run: GitHubWorkflowRun


# https://docs.github.com/en/rest/actions/workflow-jobs?apiVersion=2022-11-28#get-a-job-for-a-workflow-run
GitHubWorkflowJobConclusionType = typing.Literal[
    "success",
    "failure",
    "neutral",
    "cancelled",
    "skipped",
    "timed_out",
    "action_required",
]

GitHubWorkflowJobStepStatus = typing.Literal["queued", "in_progress", "completed"]


class GitHubWorkflowJobStep(typing_extensions.TypedDict):
    name: str
    status: GitHubWorkflowJobStepStatus
    conclusion: GitHubWorkflowJobConclusionType | None
    number: int
    started_at: ISODateTimeType | None
    completed_at: ISODateTimeType | None


class GitHubWorkflowJob(typing_extensions.TypedDict):
    id: int
    run_id: int
    name: str
    workflow_name: str
    conclusion: GitHubWorkflowJobConclusionType
    started_at: ISODateTimeType
    completed_at: ISODateTimeType
    labels: list[str]
    run_attempt: int
    steps: list[GitHubWorkflowJobStep]
    runner_id: int
    head_sha: SHAType


class GitHubEventWorkflowJob(GitHubEventWithRepository):
    action: GitHubEventPullRequestActionType
    workflow_job: GitHubWorkflowJob | None
