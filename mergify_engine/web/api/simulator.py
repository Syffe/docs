import dataclasses
import typing

import daiquiri
import fastapi
import pydantic

from mergify_engine import actions
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine.clients import http
from mergify_engine.engine import actions_runner
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.web import api
from mergify_engine.web.api import security


LOG = daiquiri.getLogger(__name__)


router = fastapi.APIRouter(
    tags=["simulator"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


class SimulatorPayload(pydantic.BaseModel):
    mergify_yml: str = pydantic.Field(description="A Mergify configuration")

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
    except actions.InvalidDynamicActionConfiguration as e:
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
