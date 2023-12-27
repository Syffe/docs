import jinja2.exceptions
import jinja2.sandbox

from mergify_engine import condition_value_querier
from mergify_engine import context
from mergify_engine import github_types


RenderTemplateFailure = condition_value_querier.RenderTemplateFailure


class DummyList(list[str]):
    def __getitem__(self, index: int) -> str:  # type: ignore [override]
        return ""


class DummyContext(context.Context):
    @staticmethod
    def ensure_complete() -> None:  # type: ignore[override]
        return None


class DummyPullRequest(condition_value_querier.PullRequest):
    # This is only used to check Jinja2 syntax validity and must be sync
    def __getattr__(  # type: ignore[override]
        self,
        name: str,
    ) -> condition_value_querier.PullRequestAttributeType:
        key = name.replace("_", "-")
        if key in condition_value_querier.PullRequest.STRING_ATTRIBUTES:
            return ""
        if key in condition_value_querier.PullRequest.NUMBER_ATTRIBUTES:
            return 0
        if key in condition_value_querier.PullRequest.BOOLEAN_ATTRIBUTES:
            return False
        if key in condition_value_querier.PullRequest.LIST_ATTRIBUTES:
            return DummyList()
        if (
            key
            in condition_value_querier.PullRequest.LIST_ATTRIBUTES_WITH_LENGTH_OPTIMIZATION
        ):
            return 0

        raise condition_value_querier.PullRequestAttributeError(key)

    def render_template(  # type: ignore[override]
        self,
        template: str,
        extra_variables: dict[str, str] | None = None,
    ) -> str:
        """Render a template interpolating variables based on pull request attributes."""
        env = jinja2.sandbox.SandboxedEnvironment(
            undefined=jinja2.StrictUndefined,
        )
        #
        env.filters["markdownify"] = lambda s: s
        env.filters["get_section"] = self.dummy_get_section

        with self._template_exceptions_mapping():
            used_variables = jinja2.meta.find_undeclared_variables(env.parse(template))
            infos = {}
            for k in used_variables:
                if extra_variables and k in extra_variables:
                    infos[k] = extra_variables[k]
                else:
                    infos[k] = getattr(self, k)
            return env.from_string(template).render(**infos)

    @staticmethod
    def dummy_get_section(v: str, section: str, default: str | None = None) -> str:  # noqa: ARG004
        return v


DUMMY_PR = DummyPullRequest(
    DummyContext(
        None,  # type: ignore[arg-type]
        github_types.GitHubPullRequest(
            {
                "node_id": "42",
                "locked": False,
                "assignees": [],
                "requested_reviewers": [],
                "requested_teams": [],
                "milestone": None,
                "title": "",
                "body": "",
                "number": github_types.GitHubPullRequestNumber(0),
                "html_url": "",
                "issue_url": "",
                "id": github_types.GitHubPullRequestId(0),
                "maintainer_can_modify": False,
                "state": "open",
                "created_at": github_types.ISODateTimeType("2021-06-01T18:41:39Z"),
                "updated_at": github_types.ISODateTimeType("2021-06-01T18:41:39Z"),
                "merged": False,
                "merged_by": None,
                "merged_at": None,
                "closed_at": None,
                "draft": False,
                "merge_commit_sha": None,
                "commits": 0,
                "mergeable_state": "unknown",
                "mergeable": None,
                "rebaseable": False,
                "changed_files": 1,
                "user": {
                    "id": github_types.GitHubAccountIdType(0),
                    "login": github_types.GitHubLogin(""),
                    "type": "User",
                    "avatar_url": "",
                },
                "labels": [],
                "base": {
                    "user": {
                        "id": github_types.GitHubAccountIdType(0),
                        "login": github_types.GitHubLogin(""),
                        "type": "User",
                        "avatar_url": "",
                    },
                    "label": github_types.GitHubBaseBranchLabel(""),
                    "ref": github_types.GitHubRefType(""),
                    "sha": github_types.SHAType(""),
                    "repo": {
                        "url": "",
                        "html_url": "",
                        "default_branch": github_types.GitHubRefType(""),
                        "full_name": "",
                        "archived": False,
                        "id": github_types.GitHubRepositoryIdType(0),
                        "private": False,
                        "name": github_types.GitHubRepositoryName(""),
                        "owner": {
                            "login": github_types.GitHubLogin(""),
                            "id": github_types.GitHubAccountIdType(0),
                            "type": "User",
                            "avatar_url": "",
                        },
                    },
                },
                "head": {
                    "user": {
                        "id": github_types.GitHubAccountIdType(0),
                        "login": github_types.GitHubLogin(""),
                        "type": "User",
                        "avatar_url": "",
                    },
                    "label": github_types.GitHubHeadBranchLabel(""),
                    "ref": github_types.GitHubRefType(""),
                    "sha": github_types.SHAType(""),
                    "repo": {
                        "url": "",
                        "html_url": "",
                        "default_branch": github_types.GitHubRefType(""),
                        "full_name": "",
                        "archived": False,
                        "id": github_types.GitHubRepositoryIdType(0),
                        "private": False,
                        "name": github_types.GitHubRepositoryName(""),
                        "owner": {
                            "login": github_types.GitHubLogin(""),
                            "id": github_types.GitHubAccountIdType(0),
                            "type": "User",
                            "avatar_url": "",
                        },
                    },
                },
            },
        ),
        [],
    ),
)
