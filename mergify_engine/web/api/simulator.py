import dataclasses
import re
import typing

import daiquiri
import fastapi
import pydantic

from mergify_engine import actions as actions_mod
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine.clients import http
from mergify_engine.engine import actions_runner
from mergify_engine.rules import conditions as rule_conditions
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import pull_request_rules as prr_mod
from mergify_engine.web import api
from mergify_engine.web.api import security


LOG = daiquiri.getLogger(__name__)


router = fastapi.APIRouter(
    tags=["simulator"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)

MergifyConfigYaml = typing.Annotated[
    str,
    pydantic.Field(
        description="A Mergify configuration",
        # It's the same limitation as https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#get-contents
        max_length=1 * 1024 * 1024,
    ),
]


class SimulatorPayload(pydantic.BaseModel):
    mergify_yml: MergifyConfigYaml

    async def get_config(
        self, repository_ctxt: context.Repository
    ) -> mergify_conf.MergifyConfig:
        try:
            return await mergify_conf.get_mergify_config_from_file(
                repository_ctxt,
                context.MergifyConfigFile(
                    {
                        "type": "file",
                        "content": "whatever",
                        "sha": github_types.SHAType("whatever"),
                        "path": github_types.GitHubFilePath(".mergify.yml"),
                        "decoded_content": self.mergify_yml,
                    }
                ),
            )
        except mergify_conf.InvalidRules as exc:
            detail = [
                {
                    "loc": ("body", "mergify_yml"),
                    "msg": mergify_conf.InvalidRules.format_error(e),
                    "type": "mergify_config_error",
                }
                for e in sorted(exc.errors, key=str)
            ]
            raise fastapi.HTTPException(status_code=422, detail=detail)


@pydantic.dataclasses.dataclass
class SimulatorResponse:
    title: str = dataclasses.field(
        metadata={"description": "The title of the Mergify check run simulation"},
    )
    summary: str = dataclasses.field(
        metadata={"description": "The summary of the Mergify check run simulation"},
    )


@router.post(
    "/repos/{owner}/{repository}/pulls/{number}/simulator",
    summary="Get a Mergify simulation for a pull request",
    description="Get a simulation of what Mergify will do on a pull request",
    response_model=SimulatorResponse,
    responses={
        **api.default_responses,  # type: ignore
    },
)
async def simulator_pull(
    body: SimulatorPayload,
    repository_ctxt: security.Repository,
    number: typing.Annotated[int, fastapi.Path(description="The pull request number")],
) -> SimulatorResponse:
    config = await body.get_config(repository_ctxt)
    try:
        ctxt = await repository_ctxt.get_pull_request_context(
            github_types.GitHubPullRequestNumber(number)
        )
    except http.HTTPClientSideError as e:
        raise fastapi.HTTPException(status_code=e.status_code, detail=e.message)
    ctxt.sources = [{"event_type": "mergify-simulator", "data": [], "timestamp": ""}]  # type: ignore[typeddict-item]
    try:
        match = await config["pull_request_rules"].get_pull_request_rules_evaluator(
            ctxt
        )
    except actions_mod.InvalidDynamicActionConfiguration as e:
        title = "The current Mergify configuration is invalid"
        summary = f"### {e.reason}\n\n{e.details}"
    else:
        title, summary = await actions_runner.gen_summary(
            ctxt,
            config["pull_request_rules"],
            match,
            display_action_configs=True,
        )
    return SimulatorResponse(title=title, summary=summary)


@router.post(
    "/repos/{owner}/{repository}/simulator",
    summary="Get a Mergify simulation for a repository",
    description="Get a simulation of what Mergify will do for this repository",
    response_model=SimulatorResponse,
    # checkout repository permissions
    dependencies=[fastapi.Depends(security.get_repository_context)],
    responses={
        **api.default_responses,  # type: ignore
    },
)
async def simulator_repo(
    body: SimulatorPayload, repository_ctxt: security.Repository
) -> SimulatorResponse:
    await body.get_config(repository_ctxt)
    return SimulatorResponse(
        title="The configuration is valid",
        summary="",
    )


@pydantic.dataclasses.dataclass
class RepositoryConfigurationSimulatorResponse:
    message: str = dataclasses.field(
        metadata={"description": "The message of the Mergify check run simulation"},
    )


@router.post(
    "/repos/{owner}/{repository}/configuration-simulator",
    summary="Get a Mergify configuration simulation for a repository",
    description="Get a simulation of what Mergify will do for this repository",
    response_model=RepositoryConfigurationSimulatorResponse,
    dependencies=[fastapi.Depends(security.get_repository_context)],
    responses={
        **api.default_responses,  # type: ignore
        200: {
            "content": {
                "application/json": {
                    "example": {"message": "The configuration is valid"}
                }
            }
        },
    },
)
async def repository_configuration_simulator(
    body: SimulatorPayload, repository_ctxt: security.Repository
) -> RepositoryConfigurationSimulatorResponse:
    await body.get_config(repository_ctxt)
    return RepositoryConfigurationSimulatorResponse(
        message="The configuration is valid"
    )


@pydantic.dataclasses.dataclass
class PullRequestRule:
    name: str = dataclasses.field(
        metadata={"description": "The pull request rule name"}
    )
    conditions: (
        rule_conditions.ConditionEvaluationResult.Serialized
    ) = dataclasses.field(metadata={"description": "The pull request rule conditions"})
    actions: dict[str, actions_mod.RawConfigT] = dataclasses.field(
        metadata={"description": "The pull request rule actions"}
    )


@pydantic.dataclasses.dataclass
class PullRequestConfigurationSimulatorResponse:
    message: str = dataclasses.field(
        metadata={"description": "The message of the Mergify check run simulation"},
    )
    pull_request_rules: list[PullRequestRule] = dataclasses.field(
        metadata={"description": "The evaluated pull request rules"}
    )

    @classmethod
    def from_pull_request_rules_evaluator(
        cls, message: str, evaluator: prr_mod.PullRequestRulesEvaluator
    ) -> "PullRequestConfigurationSimulatorResponse":
        serialized_prr = []

        for rule in evaluator.evaluated_rules:
            conditions = (
                rule_conditions.ConditionEvaluationResult.from_rule_condition_node(
                    rule.conditions.condition, filter_key=None
                )
            )

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
                )
            )

        return cls(message=message, pull_request_rules=serialized_prr)

    @staticmethod
    def _sanitize_action_config(
        config_key: str, config_value: typing.Any
    ) -> typing.Any:
        if "bot_account" in config_key and isinstance(config_value, dict):
            return config_value["login"]
        if isinstance(config_value, rule_conditions.PullRequestRuleConditions):
            return rule_conditions.ConditionEvaluationResult.from_rule_condition_node(
                config_value.condition, filter_key=None
            )
        if isinstance(config_value, re.Pattern):
            return config_value.pattern
        return config_value


@router.post(
    "/repos/{owner}/{repository}/pulls/{number}/configuration-simulator",
    summary="Get a Mergify configuration simulation for a pull request",
    description="Get a simulation of what Mergify will do on a pull request",
    include_in_schema=False,
    response_model=PullRequestConfigurationSimulatorResponse,
    responses={
        **api.default_responses,  # type: ignore
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "message": "The configuration is valid",
                        "pull_request_rules": [
                            {
                                "name": "some rule",
                                "conditions": {
                                    "label": "all of",
                                    "match": True,
                                    "subconditions": [],
                                },
                                "actions": {"some_action": {}},
                            }
                        ],
                    }
                }
            }
        },
    },
)
async def pull_request_configuration_simulator(
    body: SimulatorPayload,
    repository_ctxt: security.Repository,
    number: typing.Annotated[
        github_types.GitHubPullRequestNumber,
        fastapi.Path(description="The pull request number"),
    ],
) -> PullRequestConfigurationSimulatorResponse:
    config = await body.get_config(repository_ctxt)

    try:
        ctxt = await repository_ctxt.get_pull_request_context(
            github_types.GitHubPullRequestNumber(number)
        )
    except http.HTTPClientSideError as e:
        raise fastapi.HTTPException(status_code=e.status_code, detail=e.message)
    ctxt.sources = [{"event_type": "mergify-simulator", "data": [], "timestamp": ""}]  # type: ignore[typeddict-item]

    try:
        prr_evaluator = await config[
            "pull_request_rules"
        ].get_pull_request_rules_evaluator(ctxt)
    except actions_mod.InvalidDynamicActionConfiguration as e:
        detail = [
            {
                "loc": ("body", "mergify_yml"),
                "msg": e.reason,
                "details": e.details,
                "type": "mergify_config_error",
            }
        ]
        raise fastapi.HTTPException(status_code=422, detail=detail)

    return PullRequestConfigurationSimulatorResponse.from_pull_request_rules_evaluator(
        message="The configuration is valid", evaluator=prr_evaluator
    )
