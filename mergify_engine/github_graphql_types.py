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
    edges: typing.List[GraphqlCommentsEdge]


class GraphqlReviewThread(typing.TypedDict):
    isResolved: bool
    comments: GraphqlComments
    id: str


class GraphqlResolveReviewThread(typing.TypedDict):
    thread: GraphqlReviewThread


class GraphqlReviewThreadsEdge(typing.TypedDict):
    node: GraphqlReviewThread


class GraphqlReviewThreads(typing.TypedDict):
    edges: typing.List[GraphqlReviewThreadsEdge]


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
    nodes: typing.List[GraphqlCommentIds]


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
