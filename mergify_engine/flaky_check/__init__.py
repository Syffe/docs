from __future__ import annotations

import typing

import daiquiri
import sqlalchemy

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import refresher
from mergify_engine.ci import pull_registries
from mergify_engine.flaky_check.utils import NeedRerunStatus
from mergify_engine.models import github as gh_models


if typing.TYPE_CHECKING:
    from mergify_engine import context

LOG = daiquiri.getLogger(__name__)

ALREADY_RERUN_CACHE_PREFIX = "flaky_check_already_reran"
ALREADY_RERUN_CACHE_EXPIRE = 600
FLAKY_CHECK_NEED_REFRESH_CACHE_PREFIX = "flaky_check_need_refresh"
FLAKY_CHECK_NEED_REFRESH_CACHE_EXPIRE = 3600


class CheckToRerunResult(typing.TypedDict):
    check_id: int
    check_name: str
    check_app_slug: str
    status: NeedRerunStatus


async def get_checks_to_rerun(
    repository: context.Repository,
    checks: list[github_types.CachedGitHubCheckRun],
) -> list[CheckToRerunResult]:
    flaky_conf = repository.mergify_config["_checks_to_retry_on_failure"]

    checks_to_rerun = []

    for check in checks:
        need_rerun_status = NeedRerunStatus.DONT_NEED_RERUN
        if check["name"] in flaky_conf and check["conclusion"] == "failure":
            if check["app_slug"] == "github-actions":
                need_rerun_status = await is_gha_job_rerun_needed(
                    repository.installation.redis.cache,
                    check,
                    flaky_conf[check["name"]],
                )

        if need_rerun_status != NeedRerunStatus.DONT_NEED_RERUN:
            checks_to_rerun.append(
                CheckToRerunResult(
                    check_id=check["id"],
                    check_name=check["name"],
                    check_app_slug=check["app_slug"],
                    status=need_rerun_status,
                ),
            )

    return checks_to_rerun


async def is_gha_job_rerun_needed(
    redis_cache: redis_utils.RedisCache,
    check: github_types.CachedGitHubCheckRun,
    max_rerun: int,
) -> NeedRerunStatus:
    async with database.create_session() as session:
        need_rerun = NeedRerunStatus(
            await gh_models.WorkflowJob.is_rerun_needed(
                session,
                check["id"],
                max_rerun,
            ),
        )

    if need_rerun == NeedRerunStatus.UNKONWN:
        # NOTE(Kontrolix): It means that this job is not yet ready to evaluate.
        # Set info in cache to refresh this PR when job will be ready
        await redis_cache.set(
            f"{FLAKY_CHECK_NEED_REFRESH_CACHE_PREFIX}_{check['id']}",
            check["id"],
            ex=FLAKY_CHECK_NEED_REFRESH_CACHE_EXPIRE,
        )
    else:
        await redis_cache.delete(
            f"{FLAKY_CHECK_NEED_REFRESH_CACHE_PREFIX}_{check['id']}",
        )

    return need_rerun


async def send_pull_refresh_for_jobs(
    redis_links: redis_utils.RedisLinks,
    job_ids: list[int],
) -> None:
    need_refresh_job_ids = await filter_jobs_id_that_need_refresh(redis_links, job_ids)

    if not need_refresh_job_ids:
        return

    async with database.create_session() as session:
        jobs = await session.scalars(
            sqlalchemy.select(gh_models.WorkflowJob).where(
                gh_models.WorkflowJob.id.in_(need_refresh_job_ids),
            ),
        )

        for job in jobs:
            pg_pull_registry = pull_registries.PostgresPullRequestRegistry(
                session,
                pull_registries.HTTPPullRequestRegistry(
                    login=job.repository.owner.login,
                ),
            )
            pulls = await pg_pull_registry.get_from_commit(
                job.repository.owner.login,
                github_types.GitHubRepositoryName(job.repository.name),
                job.head_sha,
            )

            for pr in pulls:
                if pr.state == "open":
                    await refresher.send_pull_refresh(
                        redis_links.stream,
                        typing.cast(
                            github_types.GitHubRepository,
                            job.repository._as_dict(),
                        ),
                        pull_request_number=github_types.GitHubPullRequestNumber(
                            pr.number,
                        ),
                        action="internal",
                        source="WorkflowJob neighbours computed",
                    )


async def filter_jobs_id_that_need_refresh(
    redis_links: redis_utils.RedisLinks,
    job_ids: list[int],
) -> list[int]:
    keys = [f"{FLAKY_CHECK_NEED_REFRESH_CACHE_PREFIX}_{job_id}" for job_id in job_ids]
    return [
        int(job_id)
        for job_id in await redis_links.cache.mget(keys)
        if job_id is not None
    ]


async def rerun_flaky_checks(ctxt: context.Context) -> None:
    for check_rerun in ctxt.flaky_checks_to_rerun:
        if check_rerun["status"] != NeedRerunStatus.NEED_RERUN:
            continue

        if check_rerun["check_app_slug"] == "github-actions":
            await rerun_flaky_gha_job(ctxt, check_rerun)


async def rerun_flaky_gha_job(
    ctxt: context.Context,
    check_rerun: CheckToRerunResult,
) -> None:
    already_rerun_key = f"{ALREADY_RERUN_CACHE_PREFIX}_{check_rerun['check_id']}"
    if await ctxt.repository.installation.redis.cache.exists(already_rerun_key):
        return

    await ctxt.client.post(
        f"{ctxt.base_url}/actions/jobs/{check_rerun['check_id']}/rerun",
    )
    ctxt.log.info("Rerun job: %s", check_rerun["check_id"])

    await ctxt.repository.installation.redis.cache.set(
        already_rerun_key,
        "",
        ex=ALREADY_RERUN_CACHE_EXPIRE,
    )
