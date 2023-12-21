from __future__ import annotations

import dataclasses
import re
import typing

import fastapi
import pydantic
import typing_extensions
import voluptuous

from mergify_engine import actions as actions_mod
from mergify_engine import condition_value_querier
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine import pull_request_getter
from mergify_engine.clients import http
from mergify_engine.rules import conditions as rule_conditions
from mergify_engine.rules import conditions as rules_conditions
from mergify_engine.rules.config import conditions as config_conditions
from mergify_engine.rules.config import pull_request_rules as prr_mod
from mergify_engine.rules.config import queue_rules as qr_mod
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
                "description": "The pull requests of the repository that matches the given conditions",
            },
        },
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
        **api.default_responses,  # type: ignore[dict-item]
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
        {"and": validated_conditions},
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
                pull_with_all_data = await pull_request_getter.get_pull_request(
                    repository.installation.client,
                    pull["number"],
                    repo_owner=repository.installation.owner_login,
                    repo_name=repository.repo["name"],
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
                            f"{repository.base_url}/pulls/{pull['number']}",
                        ),
                    )

                matching_pulls.append(pull_with_all_data)
                if len(matching_pulls) == current_page.per_page:
                    break

    response_page: pagination.Page[github_types.GitHubPullRequest] = pagination.Page(
        items=matching_pulls,
        current=current_page,
        cursor_next=None if reached_last_page else f"{page}-{nb_pulls_on_github}",
    )

    return MatchingPullRequests(page=response_page)  # type: ignore[call-arg]


@pydantic.dataclasses.dataclass
class PullRequestRule:
    name: str = dataclasses.field(
        metadata={"description": "The pull request rule name"},
    )
    conditions: (
        rule_conditions.ConditionEvaluationResult.Serialized
    ) = dataclasses.field(metadata={"description": "The pull request rule conditions"})
    actions: dict[str, actions_mod.RawConfigT] = dataclasses.field(
        metadata={"description": "The pull request rule actions"},
    )


@pydantic.dataclasses.dataclass
class QueueRule:
    name: str = dataclasses.field(metadata={"description": "The queue rule name"})
    queue_conditions: (
        rule_conditions.QueueConditionEvaluationJsonSerialized
    ) = dataclasses.field(metadata={"description": "The queue conditions"})
    merge_conditions: (
        rule_conditions.QueueConditionEvaluationJsonSerialized
    ) = dataclasses.field(metadata={"description": "The merge conditions"})


class PullRequestSummarySerializerMixin:
    @classmethod
    def _serialize_queue_rules(
        cls,
        qr_evaluator: qr_mod.QueueRulesEvaluator,
    ) -> list[QueueRule]:
        serialized_qr = []

        for rule in qr_evaluator.evaluated_rules:
            queue_conditions = rule.queue_conditions.get_evaluation_result()
            merge_conditions = rule.merge_conditions.get_evaluation_result()

            serialized_qr.append(
                QueueRule(
                    name=rule.name,
                    queue_conditions=queue_conditions.as_json_dict(),
                    merge_conditions=merge_conditions.as_json_dict(),
                ),
            )

        return serialized_qr

    @classmethod
    def _serialize_pull_request_rules(
        cls,
        prr_evaluator: prr_mod.PullRequestRulesEvaluator,
    ) -> list[PullRequestRule]:
        serialized_prr = []

        for rule in prr_evaluator.evaluated_rules:
            if rule.hidden:
                continue

            conditions = rule.conditions.get_evaluation_result()

            actions = {}
            for name, action in rule.actions.items():
                config = {
                    key: cls._sanitize_action_config(key, value)
                    for key, value in action.executor.config.items()
                    if key not in action.executor.config_hidden_from_simulator
                }
                actions[name] = config

            serialized_prr.append(
                PullRequestRule(
                    name=rule.name,
                    conditions=conditions.serialized(),
                    actions=actions,
                ),
            )

        return serialized_prr

    @staticmethod
    def _sanitize_action_config(
        config_key: str,
        config_value: typing.Any,
    ) -> typing.Any:
        if "bot_account" in config_key and isinstance(config_value, dict):
            return config_value["login"]
        if isinstance(config_value, rule_conditions.PullRequestRuleConditions):
            return rule_conditions.ConditionEvaluationResult.from_rule_condition_node(
                config_value.condition,
                filter_key=None,
            )
        if isinstance(config_value, re.Pattern):
            return config_value.pattern
        return config_value


@pydantic.dataclasses.dataclass
class PullRequestSummaryResponse(PullRequestSummarySerializerMixin):
    pull_request_rules: list[PullRequestRule] = dataclasses.field(
        metadata={"description": "The evaluated pull request rules"},
    )
    queue_rules: list[QueueRule] = dataclasses.field(
        metadata={"description": "The evaluated queue rules"},
    )

    @classmethod
    def from_configuration_evaluators(
        cls,
        prr_evaluator: prr_mod.PullRequestRulesEvaluator,
        qr_evaluator: qr_mod.QueueRulesEvaluator,
    ) -> PullRequestSummaryResponse:
        serialized_prr = cls._serialize_pull_request_rules(prr_evaluator)
        serialized_queue_rules = cls._serialize_queue_rules(qr_evaluator)

        return cls(
            pull_request_rules=serialized_prr,
            queue_rules=serialized_queue_rules,
        )


@router.get(
    "/repos/{owner}/{repository}/pulls/{number}/summary",
    summary="Mergify summary of a pull request",
    description="Get the list of actions that Mergify will do on a pull request",
    include_in_schema=False,
    response_model=PullRequestSummaryResponse,
    responses=api.default_responses,
)
async def get_pull_request_summary(
    number: typing.Annotated[
        github_types.GitHubPullRequestNumber,
        fastapi.Path(description="The pull request number"),
    ],
    repository: security.RepositoryWithConfig,
) -> PullRequestSummaryResponse:
    try:
        ctxt = await repository.get_pull_request_context(
            github_types.GitHubPullRequestNumber(number),
        )
    except http.HTTPClientSideError as e:
        raise fastapi.HTTPException(status_code=e.status_code, detail=e.message)

    try:
        prr_evaluator = await repository.mergify_config[
            "pull_request_rules"
        ].get_pull_request_rules_evaluator(ctxt)
    except actions_mod.InvalidDynamicActionConfiguration as e:
        detail = [
            {
                "loc": ("body", "mergify_yml"),
                "msg": e.reason,
                "details": e.details,
                "type": "mergify_config_error",
            },
        ]
        raise fastapi.HTTPException(status_code=422, detail=detail)

    qr_evaluator = await repository.mergify_config[
        "queue_rules"
    ].get_queue_rules_evaluator(ctxt)

    return PullRequestSummaryResponse.from_configuration_evaluators(
        prr_evaluator=prr_evaluator,
        qr_evaluator=qr_evaluator,
    )
