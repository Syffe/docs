import datetime
import typing

import fastapi

from mergify_engine import github_types
from mergify_engine.ci import job_registries
from mergify_engine.ci import reports
from mergify_engine.web import api
from mergify_engine.web.api import security


router = fastapi.APIRouter(
    tags=["ci"],
    dependencies=[fastapi.Security(security.get_http_auth)],
)


@router.get(
    "/{owner}",
    include_in_schema=False,
    summary="Get global CI data",
    description="Get the global CI data for an organization",
    response_model=reports.ReportPayload,
    responses={
        **api.default_responses,  # type: ignore
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "total_costs": {"amount": 0.02, "currency": "USD"},
                        "categories": {
                            "deployments": {
                                "type": "deployments",
                                "total_cost": {"amount": 0.0, "currency": "USD"},
                                "dimensions": {
                                    "conclusions": {"type": "conclusions", "items": []},
                                    "jobs": {"type": "jobs", "items": []},
                                },
                                "difference": {"amount": 0.0, "currency": "USD"},
                            },
                            "scheduled_jobs": {
                                "type": "scheduled_jobs",
                                "total_cost": {"amount": 0.0, "currency": "USD"},
                                "dimensions": {
                                    "conclusions": {"type": "conclusions", "items": []},
                                    "jobs": {"type": "jobs", "items": []},
                                },
                                "difference": {"amount": 0.0, "currency": "USD"},
                            },
                            "pull_requests": {
                                "type": "pull_requests",
                                "total_cost": {"amount": 0.02, "currency": "USD"},
                                "dimensions": {
                                    "actors": {
                                        "type": "actors",
                                        "items": [
                                            {
                                                "name": "mergifyio-testing",
                                                "cost": {
                                                    "amount": 0.02,
                                                    "currency": "USD",
                                                },
                                            }
                                        ],
                                    },
                                    "jobs": {
                                        "type": "jobs",
                                        "items": [
                                            {
                                                "name": "some-job-1",
                                                "cost": {
                                                    "amount": 0.02,
                                                    "currency": "USD",
                                                },
                                            }
                                        ],
                                    },
                                    "lifecycles": {
                                        "type": "lifecycles",
                                        "items": [
                                            {
                                                "name": "Manual retry",
                                                "cost": {
                                                    "amount": 0.02,
                                                    "currency": "USD",
                                                },
                                            }
                                        ],
                                    },
                                    "conclusions": {
                                        "type": "conclusions",
                                        "items": [
                                            {
                                                "name": "failure",
                                                "cost": {
                                                    "amount": 0.02,
                                                    "currency": "USD",
                                                },
                                            }
                                        ],
                                    },
                                },
                                "difference": {"amount": 0.0, "currency": "USD"},
                            },
                        },
                        "total_difference": {"amount": 0.0, "currency": "USD"},
                    }
                }
            }
        },
    },
)
async def repository_queues(
    owner: typing.Annotated[
        github_types.GitHubLogin,
        fastapi.Path(description="The owner"),
    ],
    # TODO(charly): we can't use typing.Annotated here, FastAPI 0.95.0 has a bug with APIRouter
    # https://github.com/tiangolo/fastapi/discussions/9279
    repository: github_types.GitHubRepositoryName
    | None = fastapi.Query(  # noqa: B008
        None,
        description="The repository",
    ),
    start_at: datetime.date
    | None = fastapi.Query(  # noqa: B008
        None, description="The start of the date range"
    ),
    end_at: datetime.date
    | None = fastapi.Query(  # noqa: B008
        None,
        description="The end of the date range",
    ),
) -> reports.ReportPayload:
    job_registry = job_registries.PostgresJobRegistry()
    query = reports.Query(owner, repository, start_at, end_at)
    report = reports.Report(job_registry, query)

    return await report.run()
