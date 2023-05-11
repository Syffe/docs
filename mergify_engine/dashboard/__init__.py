from mergify_engine import github_types
from mergify_engine import settings


def get_queue_pull_request_details_url(
    pull_request: github_types.GitHubPullRequest,
) -> str:
    base_repo = pull_request["base"]["repo"]
    return f"{settings.DASHBOARD_UI_FRONT_URL}/github/{base_repo['owner']['login']}/repo/{base_repo['name']}/queues?pull={pull_request['number']}"
