import daiquiri

from mergify_engine import github_types
from mergify_engine.models import github_actions as sql_models


LOG = daiquiri.getLogger(__name__)


class HTTPJobRegistry:
    @staticmethod
    def is_workflow_job_ignored(payload: github_types.GitHubWorkflowJob) -> bool:
        return not payload["completed_at"] or payload["conclusion"] == "skipped"

    @staticmethod
    def is_workflow_run_ignored(payload: github_types.GitHubWorkflowRun) -> bool:
        if payload["conclusion"] is None:
            return True

        try:
            sql_models.WorkflowRunTriggerEvent(payload["event"])
        except ValueError:
            return True
        else:
            return False
