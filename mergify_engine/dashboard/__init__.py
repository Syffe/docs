from mergify_engine import github_types
from mergify_engine import settings


def get_queue_pull_request_details_url(
    pull_request: github_types.GitHubPullRequest,
) -> str:
    base_repo = pull_request["base"]["repo"]
    return f"{settings.DASHBOARD_UI_FRONT_URL}/github/{base_repo['owner']['login']}/repo/{base_repo['name']}/queues?pull={pull_request['number']}"


def get_eventlogs_url(
    login: github_types.GitHubLogin,
    repo_name: github_types.GitHubRepositoryName,
) -> str:
    # TODO(lecrepont01): add the pull request number to query parameters with new eventlogs version
    return (
        f"{settings.DASHBOARD_UI_FRONT_URL}/github/{login}/repo/{repo_name}/event-logs"
    )
