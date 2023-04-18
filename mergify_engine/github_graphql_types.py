import typing


# Review Threads
class CachedReviewThread(typing.TypedDict):
    isResolved: bool
    first_comment: str


class GraphqlReviewThreadComment(typing.TypedDict):
    body: str


class GraphqlCommentsEdge(typing.TypedDict):
    node: GraphqlReviewThreadComment


class GraphqlComments(typing.TypedDict):
    edges: list[GraphqlCommentsEdge]


class GraphqlReviewThread(typing.TypedDict):
    isResolved: bool
    comments: GraphqlComments
    id: str


class GraphqlResolveReviewThread(typing.TypedDict):
    thread: GraphqlReviewThread


class GraphqlReviewThreadsEdge(typing.TypedDict):
    node: GraphqlReviewThread


class GraphqlReviewThreads(typing.TypedDict):
    edges: list[GraphqlReviewThreadsEdge]


class GraphqlPullRequestForReviewThreads(typing.TypedDict):
    reviewThreads: GraphqlReviewThreads


class GraphqlResolveThreadMutationResponse(typing.TypedDict):
    resolveReviewThread: GraphqlResolveReviewThread


class GraphqlRepositoryForReviewThreads(typing.TypedDict):
    pullRequest: GraphqlPullRequestForReviewThreads


class GraphqlReviewThreadsQuery(typing.TypedDict):
    repository: GraphqlRepositoryForReviewThreads


# Hiding comments
class GraphqlCommentIds(typing.TypedDict):
    id: str
    databaseId: int


class GraphqlCommentsForHidingComments(typing.TypedDict):
    nodes: list[GraphqlCommentIds]


class GraphqlPullRequestForHidingComments(typing.TypedDict):
    comments: GraphqlCommentsForHidingComments


class GraphqlRepositoryForHidingComments(typing.TypedDict):
    pullRequest: GraphqlPullRequestForHidingComments


class GraphqlHidingCommentsQuery(typing.TypedDict):
    repository: GraphqlRepositoryForHidingComments


ReportedContentClassifiers = typing.Literal[
    "ABUSE", "DUPLICATE", "OFF_TOPIC", "OUTDATED", "RESOLVED", "SPAM"
]


class GraphqlMinimizable(typing.TypedDict):
    isMinimized: bool
    minimizedReason: typing.Literal[ReportedContentClassifiers]
    viewerCanMinimize: bool


class GraphqlMinimizedComment(typing.TypedDict):
    minimizedComment: GraphqlMinimizable


class GraphqlMinimizedCommentResponse(typing.TypedDict):
    minimizeComment: GraphqlMinimizedComment


GitHubPullRequestReviewDecision = typing.Literal[
    "APPROVED", "CHANGES_REQUESTED", "REVIEW_REQUIRED", None
]


class GraphqlBranchProtectionRuleMatchingRef(typing.TypedDict):
    name: str
    prefix: str


class GraphqlBranchProtectionRule(typing.TypedDict, total=False):
    # https://docs.github.com/en/graphql/reference/objects#branchprotectionrule
    allowsDeletions: bool
    allowsForcePushes: bool
    blocksCreations: bool
    dismissesStaleReviews: bool
    isAdminEnforced: bool
    lockBranch: bool
    matchingRefs: list[GraphqlBranchProtectionRuleMatchingRef]
    pattern: str
    requireLastPushApproval: bool
    requiredApprovingReviewCount: int
    requiredDeploymentEnvironments: list[str]
    requiredStatusCheckContexts: list[str]
    requiresApprovingReviews: bool
    requiresCodeOwnerReviews: bool
    requiresCommitSignatures: bool
    requiresConversationResolution: bool
    requiresDeployments: bool
    requiresLinearHistory: bool
    requiresStatusChecks: bool
    requiresStrictStatusChecks: bool
    restrictsPushes: bool
    restrictsReviewDismissals: bool


class GraphqlRequiredStatusCheckInput(typing.TypedDict):
    appId: str
    context: str


class CreateGraphqlBranchProtectionRule(typing.TypedDict, total=False):
    # https://docs.github.com/en/graphql/reference/mutations#createbranchprotectionrule
    allowsDeletions: bool
    allowsForcePushes: bool
    blocksCreations: bool
    bypassForcePushActorIds: list[str]
    bypassPullRequestActorIds: list[str]
    clientMutationId: str
    dismissesStaleReviews: bool
    isAdminEnforced: bool
    lockAllowsFetchAndMerge: bool
    lockBranch: bool
    pattern: str
    pushActorIds: list[str]
    repositoryId: str
    requireLastPushApproval: bool
    requiredApprovingReviewCount: int
    requiredDeploymentEnvironments: list[str]
    requiredStatusCheckContexts: list[str]
    requiredStatusChecks: list[GraphqlRequiredStatusCheckInput]
    requiresApprovingReviews: bool
    requiresCodeOwnerReviews: bool
    requiresCommitSignatures: bool
    requiresConversationResolution: bool
    requiresDeployments: bool
    requiresLinearHistory: bool
    requiresStatusChecks: bool
    requiresStrictStatusChecks: bool
    restrictsPushes: bool
    restrictsReviewDismissals: bool
    reviewDismissalActorIds: list[str]
