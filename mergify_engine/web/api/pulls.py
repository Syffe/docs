from __future__ import annotations

import typing

import fastapi
import pydantic
import typing_extensions
import voluptuous

from mergify_engine import condition_value_querier
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine.rules import conditions as rules_conditions
from mergify_engine.rules.config import conditions as config_conditions
from mergify_engine.web import api
from mergify_engine.web.api import security


router = fastapi.APIRouter(
    tags=["pull_requests"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


class MatchingPullRequests(pagination.PageResponse[github_types.GitHubPullRequest]):
    items_key: typing.ClassVar[str] = "pull_requests"
    pull_requests: list[github_types.GitHubPullRequest] = pydantic.Field(
        json_schema_extra={
            "metadata": {
                "description": "The pull requests of the repository that matches the given conditions"
            }
        }
    )


InputConditionDict = typing_extensions.TypedDict(
    "InputConditionDict",
    {
        "and": "InputConditions",
        "or": "InputConditions",
        "not": "InputConditions",
    },
)

InputConditions = list[InputConditionDict | str]


@router.post(
    "/repos/{owner}/{repository}/pulls",
    summary="Repository's pull requests matching input conditions",
    description="List a repository's pull requests that matches the given conditions",
    include_in_schema=False,
    response_model=MatchingPullRequests,
    responses={
        **api.default_responses,  # type: ignore
        200: {
            "headers": pagination.LinkHeader,
        },
    },
)
async def get_pull_requests(
    repository: security.Repository,
    current_page: pagination.CurrentPage,
    input_body: typing.Annotated[InputConditions, fastapi.Body()],
) -> MatchingPullRequests:
    try:
        validated_conditions = [
            config_conditions.RuleConditionSchema(cond) for cond in input_body
        ]
    except voluptuous.Invalid as e:
        raise fastapi.HTTPException(
            status_code=400,
            detail=str(e),
        )

    cursor = current_page.cursor
    if cursor:
        try:
            start_page, start_pr = map(int, cursor.split("-"))
        except ValueError:
            raise fastapi.HTTPException(status_code=400, detail="Invalid page cursor")
    else:
        start_page = 1
        start_pr = 0

    matching_pulls: list[github_types.GitHubPullRequest] = []

    base_pull_conditions = rules_conditions.RuleConditionCombination(
        {"and": validated_conditions}
    )

    needs_full_data = False
    for cond in base_pull_conditions.walk():
        # The pull object from `/pulls` doesn't have all the data required
        # for those attributes to be properly evaluated
        if cond.get_attribute_name(with_length_operator=True) in (
            "conflict",
            "#commits",
            "#files",
        ):
            needs_full_data = True
            break

    # `- 1` because we'll increment the `page` at the start of the loop
    # for easier control and more easily make the `cursor_next`
    page = start_page - 1
    reached_last_page = False
    while len(matching_pulls) != current_page.per_page and not reached_last_page:
        # The number of pr we went through while trying to fill our page.
        # It will be useful for building `cursor_next`.
        nb_pulls_on_github = 0
        page += 1
        pulls = await repository.get_pulls(
            state="open",
            sort="created",
            sort_direction="desc",
            page=page,
        )
        reached_last_page = len(pulls) < current_page.per_page
        for idx, pull in enumerate(pulls):
            if page == start_page and idx < start_pr:
                continue

            pull_with_all_data = None
            nb_pulls_on_github += 1

            # Those attributes are not present when querying `/pulls`.
            # But we can manually fill them to not have to make a query to GitHub
            # since we queried only the "open" pull requests.
            pull["merged"] = False
            pull["merged_by"] = None

            if needs_full_data:
                pull_with_all_data = typing.cast(
                    github_types.GitHubPullRequest,
                    await repository.installation.client.item(
                        f"{repository.base_url}/pulls/{pull['number']}"
                    ),
                )
                pull_context = context.Context(repository, pull_with_all_data)
            else:
                pull_context = context.Context(repository, pull)

            pull_obj = condition_value_querier.PullRequest(pull_context)

            pull_conditions = base_pull_conditions.copy()
            match = await pull_conditions(pull_obj)
            if match:
                if pull_with_all_data is None:
                    pull_with_all_data = typing.cast(
                        github_types.GitHubPullRequest,
                        await repository.installation.client.item(
                            f"{repository.base_url}/pulls/{pull['number']}"
                        ),
                    )

                matching_pulls.append(pull_with_all_data)
                if len(matching_pulls) == current_page.per_page:
                    break

    response_page: pagination.Page[github_types.GitHubPullRequest] = pagination.Page(
        items=matching_pulls,
        current=current_page,
        total=len(matching_pulls),
        cursor_next=None if reached_last_page else f"{page}-{nb_pulls_on_github}",
    )

    return MatchingPullRequests(page=response_page)  # type: ignore[call-arg]
