#
# Current Redis layout:
#
#
#   +----------------+             +-----------------+                +-------------------+
#   |                |             |                 |                |                   |
#   |   stream       +-------------> Org 1           +----------------+  PR #123          |
#   |                +-            |                 +-               |                   |
#   +----------------+ \--         +-----------------+ \---           +-------------------+
#     Set of orgs         \--                              \--
#     to processs            \-    +-----------------+        \--     +-------------------+
#     key = org name           \-- |                 |           \--- |                   |
#     score = timestamp           \+ Org 2           |               \+ PR #456           |
#                                  |                 |                |                   |
#                                  +-----------------+                +-------------------+
#                                  Set of pull requests               Stream with appended
#                                  to process for each                GitHub events.
#                                  org                                PR #0 is for events with
#                                  key = pull request                 no PR number attached.
#                                  score = prio + timestamp
#
#
# Orgs key format: f"bucket~{owner_id}"
# Pull key format: f"bucket-sources~{repo_id}~{pull_number or 0}"
#

from collections import abc
import contextlib
import dataclasses
import datetime
import time
import typing

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
from ddtrace import tracer
import first
import msgpack
from redis import exceptions as redis_exceptions
import sentry_sdk

from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import date
from mergify_engine import engine
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import pull_request_finder
from mergify_engine import redis_utils
from mergify_engine import refresher
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import worker_pusher
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.queue import merge_train
from mergify_engine.worker import stream_lua


# we keep the PR in queue for ~ 7 minutes (a try == WORKER_PROCESSING_DELAY)
MAX_RETRIES: int = 15
STREAM_ATTEMPTS_LOGGING_THRESHOLD: int = 20
DEDICATED_WORKERS_KEY = "dedicated-workers"
ATTEMPTS_KEY = "attempts"

LOG = daiquiri.getLogger(__name__)


class IgnoredException(Exception):
    pass


@dataclasses.dataclass
class PullRetry(Exception):
    attempts: int


class PullFastRetry(PullRetry):
    pass


class MaxPullRetry(PullRetry):
    pass


@dataclasses.dataclass
class OrgBucketRetry(Exception):
    bucket_org_key: stream_lua.BucketOrgKeyType
    attempts: int
    retry_at: datetime.datetime


class OrgBucketUnused(Exception):
    bucket_org_key: stream_lua.BucketOrgKeyType


@dataclasses.dataclass
class UnexpectedPullRetry(Exception):
    pass


T_MessagePayload = typing.NewType("T_MessagePayload", dict[bytes, bytes])
# FIXME(sileht): redis returns bytes, not str
T_MessageID = typing.NewType("T_MessageID", str)


@dataclasses.dataclass
class PullRequestBucket:
    sources_key: stream_lua.BucketSourcesKeyType
    score: float
    repo_id: github_types.GitHubRepositoryIdType
    pull_number: github_types.GitHubPullRequestNumber

    @property
    def priority(self) -> worker_pusher.Priority:
        return worker_pusher.get_priority_level_from_score(self.score)


def order_messages_without_pull_numbers(
    data: tuple[bytes, context.T_PayloadEventSource, str]
) -> str:
    # NOTE(sileht): put push event first to make PullRequestFinder more efficient.
    if data[1]["event_type"] == "push":
        return ""

    return data[1]["timestamp"]


async def run_engine(
    installation: context.Installation,
    repo_id: github_types.GitHubRepositoryIdType,
    tracing_repo_name: github_types.GitHubRepositoryNameForTracing,
    pull_number: github_types.GitHubPullRequestNumber,
    sources: list[context.T_PayloadEventSource],
) -> None:
    logger = daiquiri.getLogger(
        __name__,
        gh_repo=tracing_repo_name,
        gh_owner=installation.owner_login,
        gh_pull=pull_number,
    )
    logger.debug("engine in thread start")
    try:
        started_at = date.utcnow()
        try:
            ctxt = await installation.get_pull_request_context(
                repo_id,
                pull_number,
                # NOTE(sileht): A pull request may be reevaluated during one call of
                # consume_buckets(), so we need to clear the sources/_cache/pull/... to
                # ensure we get the last snapshot of the pull request
                force_new=True,
            )
        except http.HTTPNotFound:
            # NOTE(sileht): Don't fail if we received an event on repo/pull that doesn't exists anymore
            logger.debug("pull request doesn't exists, skipping it")
            return

        except http.HTTPClientSideError as e:
            if (
                e.status_code == 422
                and "The request could not be processed because too many files changed"
                in e.message
            ):
                logger.warning(
                    "This pull request cannot be evaluated by Mergify", exc_info=True
                )
                return

            # NOTE(sileht): Reraise to retry with worker or create a Sentry
            # issue
            raise

        if ctxt.repository.repo["archived"]:
            logger.debug("repository archived, skipping it")
            return

        try:
            result = await engine.run(ctxt, sources)
        except exceptions.UnprocessablePullRequest as e:
            logger.warning(
                "This pull request cannot be evaluated by Mergify", exc_info=True
            )
            result = check_api.Result(
                check_api.Conclusion.FAILURE,
                title="This pull request cannot be evaluated by Mergify",
                summary=e.reason,
            )
        finally:
            # NOTE(sileht): We reset sources so if the pull request is reused
            # from cache later, we will not have side effect depending on
            # previous evaluation
            ctxt.sources = []

        if ctxt.github_has_pending_background_jobs:
            # NOTE(sileht): This pull request may change its state soon and we
            # will not be aware, so refresh it later
            await refresher.send_pull_refresh(
                redis_stream=ctxt.repository.installation.redis.stream,
                repository=ctxt.repository.repo,
                pull_request_number=ctxt.pull["number"],
                priority=worker_pusher.Priority.low,
                action="internal",
                source="engine",
            )

        if result is not None:
            result.started_at = started_at
            result.ended_at = date.utcnow()
            await ctxt.set_summary_check(result)

    finally:
        logger.debug("engine in thread end")


@dataclasses.dataclass
class OwnerLoginsCache:
    # NOTE(sileht): This could take some memory in the future
    # we can assume github_types.GitHubLogin is ~ 104 bytes
    # and github_types.GitHubAccountIdType 32 bytes
    # and 10% dict overhead
    _mapping: dict[
        github_types.GitHubAccountIdType, github_types.GitHubLogin
    ] = dataclasses.field(default_factory=dict, repr=False)

    def set(
        self,
        owner_id: github_types.GitHubAccountIdType,
        owner_login: github_types.GitHubLogin,
    ) -> None:
        self._mapping[owner_id] = owner_login

    def get(
        self, owner_id: github_types.GitHubAccountIdType
    ) -> github_types.GitHubLoginForTracing:
        return self._mapping.get(
            owner_id, github_types.GitHubLoginUnknown(f"<unknown {owner_id}>")
        )


@dataclasses.dataclass
class Processor:
    redis_links: redis_utils.RedisLinks
    worker_id: str
    dedicated_owner_id: github_types.GitHubAccountIdType | None
    owners_cache: OwnerLoginsCache
    retry_unhandled_exception_forever: bool = True

    @contextlib.asynccontextmanager
    async def _translate_exception_to_retries(
        self,
        bucket_org_key: stream_lua.BucketOrgKeyType,
        bucket_sources_key: stream_lua.BucketSourcesKeyType | None = None,
    ) -> abc.AsyncIterator[None]:
        try:
            yield
        except Exception as e:
            if isinstance(e, redis_exceptions.ConnectionError):
                statsd.increment("redis.client.connection.errors")

            if exceptions.should_be_ignored(e):
                if bucket_sources_key:
                    await self.redis_links.stream.hdel(ATTEMPTS_KEY, bucket_sources_key)
                await self.redis_links.stream.hdel(ATTEMPTS_KEY, bucket_org_key)
                raise IgnoredException()

            if (
                isinstance(e, http.HTTPServerSideError)
                and bucket_sources_key is not None
            ):
                if bucket_sources_key is None:
                    raise RuntimeError("bucket_sources_key must be set at this point")

                attempts = await self.redis_links.stream.hincrby(
                    ATTEMPTS_KEY, bucket_sources_key
                )
                if attempts < MAX_RETRIES:
                    raise PullRetry(attempts) from e
                else:
                    await self.redis_links.stream.hdel(ATTEMPTS_KEY, bucket_sources_key)
                    raise MaxPullRetry(attempts) from e

            if isinstance(e, exceptions.MergifyNotInstalled):
                if bucket_sources_key:
                    await self.redis_links.stream.hdel(ATTEMPTS_KEY, bucket_sources_key)
                await self.redis_links.stream.hdel(ATTEMPTS_KEY, bucket_org_key)
                raise OrgBucketUnused(bucket_org_key)

            if isinstance(e, exceptions.RateLimited):
                retry_at = date.utcnow() + e.countdown
                score = retry_at.timestamp()
                if bucket_sources_key:
                    await self.redis_links.stream.hdel(ATTEMPTS_KEY, bucket_sources_key)
                await self.redis_links.stream.hdel(ATTEMPTS_KEY, bucket_org_key)
                await self.redis_links.stream.zadd(
                    "streams", {bucket_org_key: score}, xx=True
                )
                raise OrgBucketRetry(bucket_org_key, 0, retry_at)

            backoff = exceptions.need_retry(e)
            if backoff is None:
                # NOTE(sileht): This is our fault, so retry until we fix the bug but
                # without increasing the attempts
                raise

            attempts = await self.redis_links.stream.hincrby(
                ATTEMPTS_KEY, bucket_org_key
            )
            retry_in = 2 ** min(attempts, 3) * backoff
            retry_at = date.utcnow() + retry_in
            score = retry_at.timestamp()
            await self.redis_links.stream.zadd(
                "streams", {bucket_org_key: score}, xx=True
            )
            raise OrgBucketRetry(bucket_org_key, attempts, retry_at)

    async def consume(
        self,
        bucket_org_key: stream_lua.BucketOrgKeyType,
        owner_id: github_types.GitHubAccountIdType,
        owner_login_for_tracing: github_types.GitHubLoginForTracing,
    ) -> None:
        LOG.debug("consuming org bucket", gh_owner=owner_login_for_tracing)

        try:
            async with self._translate_exception_to_retries(bucket_org_key):
                sub = await subscription.Subscription.get_subscription(
                    self.redis_links.cache, owner_id
                )

                if not sub.features:
                    LOG.info(
                        "organization doesn't have any features enabled, skipping",
                        gh_owner_id=owner_id,
                        gh_owner=owner_login_for_tracing,
                        subscription_reason=sub.reason,
                    )
                    raise OrgBucketUnused(bucket_org_key)

                if sub.has_feature(subscription.Features.DEDICATED_WORKER):
                    if self.dedicated_owner_id is None:
                        # Spawn a worker
                        LOG.info(
                            "shared worker got an event for dedicated worker, adding org to Redis dedicated workers set",
                            owner_id=owner_id,
                            owner_login=owner_login_for_tracing,
                        )
                        await self.redis_links.stream.sadd(
                            DEDICATED_WORKERS_KEY, owner_id
                        )
                        return
                else:
                    if self.dedicated_owner_id is not None:
                        # Drop this worker
                        LOG.info(
                            "dedicated worker got an event for shared worker, removing org from Redis dedicated workers set",
                            owner_id=owner_id,
                            owner_login=owner_login_for_tracing,
                        )
                        await self.redis_links.stream.srem(
                            DEDICATED_WORKERS_KEY, owner_id
                        )
                        return

                installation_raw = await github.get_installation_from_account_id(
                    owner_id
                )
                async with github.aget_client(
                    installation_raw,
                    extra_metrics=sub.has_feature(
                        subscription.Features.PRIVATE_REPOSITORY
                    ),
                ) as client:
                    installation = context.Installation(
                        installation_raw,
                        sub,
                        client,
                        self.redis_links,
                    )
                    owner_login_for_tracing = installation.owner_login
                    self.owners_cache.set(
                        installation.owner_id,
                        owner_login_for_tracing,
                    )

                    # Sync Sentry scope and Datadog span with final gh_owner
                    root_span = tracer.current_root_span()
                    if root_span:
                        root_span.resource = owner_login_for_tracing
                        root_span.set_tag("gh_owner", owner_login_for_tracing)
                    sentry_sdk.set_tag("gh_owner", owner_login_for_tracing)
                    sentry_sdk.set_user({"username": owner_login_for_tracing})

                    pulls_processed = await self._consume_buckets(
                        bucket_org_key, installation
                    )

                    # NOTE(sileht): we don't need to refresh convoys if nothing changed
                    if pulls_processed > 0:
                        await merge_train.Convoy.refresh_convoys(installation)

        except redis_exceptions.ConnectionError:
            statsd.increment("redis.client.connection.errors")
            LOG.warning(
                "Stream Processor lost Redis connection",
                bucket_org_key=bucket_org_key,
            )
        except OrgBucketUnused:
            LOG.info(
                "unused org bucket, dropping it",
                gh_owner=owner_login_for_tracing,
                exc_info=True,
            )
            try:
                await stream_lua.drop_bucket(self.redis_links.stream, bucket_org_key)
            except redis_exceptions.ConnectionError:
                statsd.increment("redis.client.connection.errors")
                LOG.warning(
                    "fail to drop org bucket, it will be retried",
                    gh_owner=owner_login_for_tracing,
                    bucket_org_key=bucket_org_key,
                )
        except OrgBucketRetry as e:
            log_method = LOG.info
            if e.attempts >= STREAM_ATTEMPTS_LOGGING_THRESHOLD:
                if isinstance(
                    e.__cause__,
                    http.HTTPServerSideError | redis_exceptions.ConnectionError,
                ):
                    log_method = LOG.warning
                else:
                    log_method = LOG.error

            log_method(
                "failed to process org bucket, retrying",
                attempts=e.attempts,
                retry_at=e.retry_at,
                gh_owner=owner_login_for_tracing,
                exc_info=True,
            )
            return
        except Exception:
            LOG.error(
                "failed to process org bucket",
                gh_owner=owner_login_for_tracing,
                exc_info=True,
            )
            # NOTE(sileht): During functionnal tests, we don't want to retry for ever
            # so we catch the error and print all events that can't be processed
            if not self.retry_unhandled_exception_forever:
                buckets = await self.redis_links.stream.zrangebyscore(
                    bucket_org_key, min=0, max="+inf", start=0, num=1
                )
                for bucket in buckets:
                    messages = await self.redis_links.stream.xrange(bucket)
                    for _, message in messages:
                        LOG.info("event dropped", msgpack.unpackb(message[b"source"]))
                    await self.redis_links.stream.delete(bucket)
                    await self.redis_links.stream.delete(ATTEMPTS_KEY)
                    await self.redis_links.stream.zrem(bucket_org_key, bucket)

        LOG.debug("cleanup org bucket start", bucket_org_key=bucket_org_key)
        try:
            await stream_lua.clean_org_bucket(
                self.redis_links.stream,
                bucket_org_key,
                date.utcnow(),
            )
        except redis_exceptions.ConnectionError:
            statsd.increment("redis.client.connection.errors")
            LOG.warning(
                "fail to cleanup org bucket, it maybe partially replayed",
                bucket_org_key=bucket_org_key,
                gh_owner=owner_login_for_tracing,
            )
        LOG.debug(
            "cleanup org bucket end",
            bucket_org_key=bucket_org_key,
            gh_owner=owner_login_for_tracing,
        )

    @staticmethod
    def _extract_infos_from_bucket_sources_key(
        bucket_sources_key: stream_lua.BucketSourcesKeyType,
    ) -> tuple[
        github_types.GitHubRepositoryIdType,
        github_types.GitHubPullRequestNumber,
    ]:
        parts = bucket_sources_key.split("~")
        if len(parts) == 3:
            _, repo_id, pull_number = parts
        else:
            # TODO(sileht): old format remove in 5.0.0
            _, repo_id, _, pull_number = parts
        return (
            github_types.GitHubRepositoryIdType(int(repo_id)),
            github_types.GitHubPullRequestNumber(int(pull_number)),
        )

    async def select_pull_request_bucket(
        self,
        bucket_org_key: stream_lua.BucketOrgKeyType,
    ) -> PullRequestBucket | None:
        bucket_sources_keys: list[
            tuple[bytes, float]
        ] = await self.redis_links.stream.zrangebyscore(
            bucket_org_key,
            min=0,
            max="+inf",
            withscores=True,
        )

        now = date.utcnow()
        for _bucket_sources_key, _bucket_score in bucket_sources_keys:
            when = worker_pusher.get_date_from_score(_bucket_score)
            if when >= now:
                continue

            bucket_sources_key = stream_lua.BucketSourcesKeyType(
                _bucket_sources_key.decode()
            )
            (
                repo_id,
                pull_number,
            ) = self._extract_infos_from_bucket_sources_key(bucket_sources_key)
            return PullRequestBucket(
                bucket_sources_key, _bucket_score, repo_id, pull_number
            )
        return None

    async def _consume_buckets(
        self,
        bucket_org_key: stream_lua.BucketOrgKeyType,
        installation: context.Installation,
    ) -> int:
        pr_finder = pull_request_finder.PullRequestFinder(installation)

        pulls_processed = 0
        started_at = time.monotonic()
        while True:
            bucket = await self.select_pull_request_bucket(bucket_org_key)
            if bucket is None:
                break

            if (
                time.monotonic() - started_at
            ) >= settings.BUCKET_PROCESSING_MAX_SECONDS:
                statsd.increment(
                    "engine.buckets.preempted",
                    tags=[f"priority:{bucket.priority.name}"],
                )
                break

            pulls_processed += 1

            messages = await stream_lua.get_pull_messages(
                self.redis_links.stream,
                bucket_org_key,
                bucket.sources_key,
                int(constants.NORMAL_DELAY_BETWEEN_SAME_PULL_REQUEST.total_seconds())
                * worker_pusher.SCORE_TIMESTAMP_PRECISION,
            )
            statsd.histogram("engine.buckets.events.read_size", len(messages))

            if messages:
                # TODO(sileht): 4.x.x, will have repo_name optional
                # we can always pick the first one on 5.x.x milestone.
                tracing_repo_name_bin = first.first(
                    m[1].get(b"repo_name") for m in messages
                )
                tracing_repo_name: github_types.GitHubRepositoryNameForTracing
                if tracing_repo_name_bin is None:
                    tracing_repo_name = github_types.GitHubRepositoryNameUnknown(
                        f"<unknown {bucket.repo_id}>"
                    )
                else:
                    tracing_repo_name = typing.cast(
                        github_types.GitHubRepositoryNameForTracing,
                        tracing_repo_name_bin.decode(),
                    )
            else:
                tracing_repo_name = github_types.GitHubRepositoryNameUnknown(
                    f"<unknown {bucket.repo_id}>"
                )

            logger = daiquiri.getLogger(
                __name__,
                gh_owner=installation.owner_login,
                gh_repo=tracing_repo_name,
                gh_pull=bucket.pull_number,
            )
            logger.debug("read org bucket", sources=len(messages))
            if not messages:
                # Should not occur but better be safe than sorry
                await stream_lua.remove_pull(
                    self.redis_links.stream,
                    bucket_org_key,
                    bucket.sources_key,
                    (),
                )
                break

            if bucket.sources_key.endswith("~0"):
                with tracer.trace(
                    "check-runs/push pull requests finder",
                    span_type="worker",
                    resource=f"{installation.owner_login}/{tracing_repo_name}",
                ) as span:
                    logger.debug(
                        "unpack events without pull request number", count=len(messages)
                    )

                    decoded_messages = (
                        (
                            message_id,
                            typing.cast(
                                context.T_PayloadEventSource,
                                msgpack.unpackb(message[b"source"]),
                            ),
                            message[b"score"].decode(),
                        )
                        for message_id, message in messages
                    )

                    # NOTE(sileht): we put push event first to make PullsFinder
                    # more effective.
                    message_ids_to_delete = set()
                    try:
                        for message_id, source, score in sorted(
                            decoded_messages, key=order_messages_without_pull_numbers
                        ):
                            converted_messages = await self._convert_event_to_messages(
                                installation,
                                pr_finder,
                                bucket.repo_id,
                                tracing_repo_name,
                                source,
                                score,
                            )
                            message_ids_to_delete.add(
                                typing.cast(T_MessageID, message_id)
                            )
                            logger.debug(
                                "assiociated non pull request event to pull requests",
                                event_type=source["event_type"],
                                pull_requests_found=converted_messages,
                                event=source["data"],
                            )
                            statsd.increment(
                                "engine.buckets.pull_request_associations",
                                tags=[
                                    f"worker_id:{self.worker_id}",
                                    f"event_type:{source['event_type']}",
                                ],
                            )
                    finally:
                        await stream_lua.remove_pull(
                            self.redis_links.stream,
                            bucket_org_key,
                            bucket.sources_key,
                            tuple(message_ids_to_delete),
                        )
            else:
                sources = [
                    typing.cast(
                        context.T_PayloadEventSource,
                        msgpack.unpackb(message[b"source"]),
                    )
                    for _, message in messages
                ]
                message_ids = [
                    typing.cast(T_MessageID, message_id) for message_id, _ in messages
                ]
                logger.debug(
                    "consume pull request",
                    count=len(messages),
                    sources=sources,
                    message_ids=message_ids,
                )
                try:
                    with tracer.trace(
                        "pull processing",
                        span_type="worker",
                        resource=f"{installation.owner_login}/{tracing_repo_name}/{bucket.pull_number}",
                    ) as span:
                        span.set_tags(
                            {
                                "gh_repo": tracing_repo_name,
                                "gh_pull": bucket.pull_number,
                            }
                        )
                        await self._consume_pull(
                            bucket_org_key,
                            installation,
                            bucket,
                            tracing_repo_name,
                            message_ids,
                            sources,
                        )
                except OrgBucketRetry:
                    raise
                except OrgBucketUnused:
                    raise
                except PullFastRetry:
                    await self.redis_links.stream.zadd(
                        bucket_org_key,
                        {
                            bucket.sources_key: worker_pusher.get_priority_score(
                                bucket.priority,
                                constants.MIN_DELAY_BETWEEN_SAME_PULL_REQUEST,
                            )
                        },
                    )
                except (PullRetry, UnexpectedPullRetry):
                    # NOTE(sileht): Will be retried automatically in NORMAL_DELAY_BETWEEN_SAME_PULL_REQUEST
                    pass

        statsd.histogram("engine.buckets.read_size", pulls_processed)
        return pulls_processed

    async def _convert_event_to_messages(
        self,
        installation: context.Installation,
        pr_finder: pull_request_finder.PullRequestFinder,
        repo_id: github_types.GitHubRepositoryIdType,
        tracing_repo_name: github_types.GitHubRepositoryNameForTracing,
        source: context.T_PayloadEventSource,
        score: str | None = None,
    ) -> int:
        # NOTE(sileht): the event is incomplete (push, refresh, checks, status)
        # So we get missing pull numbers, add them to the stream to
        # handle retry later, add them to message to run engine on them now,
        # and delete the current message_id as we have unpack this incomplete event into
        # multiple complete event
        try:
            pull_numbers = await pr_finder.extract_pull_numbers_from_event(
                repo_id,
                source["event_type"],
                source["data"],
            )
        except Exception as e:
            if exceptions.should_be_ignored(e):
                return 0
            raise

        # NOTE(sileht): refreshing all opened pull request because something got merged
        # has a lower priority
        if source["event_type"] == "push":
            score = worker_pusher.get_priority_score(worker_pusher.Priority.low)

        pipe = typing.cast(
            redis_utils.PipelineStream, await self.redis_links.stream.pipeline()
        )
        for pull_number in pull_numbers:
            if pull_number is None:
                # NOTE(sileht): even it looks not possible, this is a safeguard to ensure
                # we didn't generate a ending loop of events, because when pull_number is
                # None, this method got called again and again.
                raise RuntimeError("Got an empty pull number")
            await worker_pusher.push(
                pipe,
                installation.owner_id,
                installation.owner_login,
                repo_id,
                tracing_repo_name,
                pull_number,
                source["event_type"],
                source["data"],
                score=score,
            )
        await pipe.execute()
        return len(pull_numbers)

    async def _consume_pull(
        self,
        bucket_org_key: stream_lua.BucketOrgKeyType,
        installation: context.Installation,
        bucket: PullRequestBucket,
        tracing_repo_name: github_types.GitHubRepositoryNameForTracing,
        message_ids: list[T_MessageID],
        sources: list[context.T_PayloadEventSource],
    ) -> None:
        for source in sources:
            # backward compat <= 7.2.1
            if "timestamp" not in source:
                continue  # type: ignore[unreachable]

            if "initial_score" in source:
                priority = worker_pusher.get_priority_level_from_score(
                    source["initial_score"]
                )
            else:
                # backward compat <= 7.2.1
                priority = bucket.priority  # type: ignore[unreachable]

            statsd.histogram(
                "engine.buckets.events.latency",
                (
                    date.utcnow() - date.fromisoformat(source["timestamp"])
                ).total_seconds(),
                tags=[
                    f"worker_id:{self.worker_id}",
                    f"priority:{priority.name}",
                ],
            )

        logger = daiquiri.getLogger(
            __name__,
            gh_repo=tracing_repo_name,
            gh_owner=installation.owner_login,
            gh_pull=bucket.pull_number,
        )

        try:
            async with self._translate_exception_to_retries(
                bucket_org_key,
                bucket.sources_key,
            ):
                await run_engine(
                    installation,
                    bucket.repo_id,
                    tracing_repo_name,
                    bucket.pull_number,
                    sources,
                )
            await self.redis_links.stream.hdel(ATTEMPTS_KEY, bucket.sources_key)
            await stream_lua.remove_pull(
                self.redis_links.stream,
                bucket_org_key,
                bucket.sources_key,
                tuple(message_ids),
            )
        except IgnoredException:
            await stream_lua.remove_pull(
                self.redis_links.stream,
                bucket_org_key,
                bucket.sources_key,
                tuple(message_ids),
            )
            logger.debug("failed to process pull request, ignoring", exc_info=True)
        except MaxPullRetry as e:
            await stream_lua.remove_pull(
                self.redis_links.stream,
                bucket_org_key,
                bucket.sources_key,
                tuple(message_ids),
            )
            statsd.increment(
                "engine.buckets.abandoning",
                tags=[f"worker_id:{self.worker_id}"],
            )
            logger.warning(
                "failed to process pull request, abandoning",
                attempts=e.attempts,
                exc_info=True,
            )
        except PullRetry as e:
            logger.info(
                "failed to process pull request, retrying",
                attempts=e.attempts,
                exc_info=True,
            )
            raise
        except OrgBucketRetry:
            raise
        except OrgBucketUnused:
            raise
        except redis_exceptions.ConnectionError:
            raise
        except Exception:
            logger.error("failed to process pull request", exc_info=True)
            if self.retry_unhandled_exception_forever:
                # Ignore it, it will retried later
                raise UnexpectedPullRetry()

            raise


async def get_dedicated_worker_owner_ids_from_redis(
    redis_stream: redis_utils.RedisStream,
) -> set[github_types.GitHubAccountIdType]:
    dedicated_workers_data = await redis_stream.smembers(DEDICATED_WORKERS_KEY)
    return {github_types.GitHubAccountIdType(int(v)) for v in dedicated_workers_data}
