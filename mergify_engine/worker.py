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

import argparse
import asyncio
import collections
from collections import abc
import contextlib
import dataclasses
import datetime
import functools
import hashlib
import itertools
import os
import signal
import time
import typing

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
from ddtrace import tracer
import first
import msgpack
from redis import exceptions as redis_exceptions
import sentry_sdk
import tenacity

from mergify_engine import check_api
from mergify_engine import config
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import date
from mergify_engine import delayed_refresh
from mergify_engine import engine
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import logs
from mergify_engine import pull_request_finder
from mergify_engine import redis_utils
from mergify_engine import refresher
from mergify_engine import service
from mergify_engine import signals
from mergify_engine import worker_lua
from mergify_engine import worker_pusher
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.dashboard import subscription
from mergify_engine.queue import merge_train


LOG = daiquiri.getLogger(__name__)

# we keep the PR in queue for ~ 7 minutes (a try == WORKER_PROCESSING_DELAY)
MAX_RETRIES: int = 15
STREAM_ATTEMPTS_LOGGING_THRESHOLD: int = 20
DEDICATED_WORKERS_KEY = "dedicated-workers"
ATTEMPTS_KEY = "attempts"


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
    bucket_org_key: worker_lua.BucketOrgKeyType
    attempts: int
    retry_at: datetime.datetime


class OrgBucketUnused(Exception):
    bucket_org_key: worker_lua.BucketOrgKeyType


@dataclasses.dataclass
class UnexpectedPullRetry(Exception):
    pass


T_MessagePayload = typing.NewType("T_MessagePayload", dict[bytes, bytes])
# FIXME(sileht): redis returns bytes, not str
T_MessageID = typing.NewType("T_MessageID", str)


@dataclasses.dataclass
class PullRequestBucket:
    sources_key: worker_lua.BucketSourcesKeyType
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
    else:
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
class StreamProcessor:
    redis_links: redis_utils.RedisLinks
    worker_id: str
    dedicated_owner_id: github_types.GitHubAccountIdType | None
    owners_cache: OwnerLoginsCache
    retry_unhandled_exception_forever: bool = True

    @contextlib.asynccontextmanager
    async def _translate_exception_to_retries(
        self,
        bucket_org_key: worker_lua.BucketOrgKeyType,
        bucket_sources_key: worker_lua.BucketSourcesKeyType | None = None,
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
        bucket_org_key: worker_lua.BucketOrgKeyType,
        owner_id: github_types.GitHubAccountIdType,
        owner_login_for_tracing: github_types.GitHubLoginForTracing,
    ) -> None:
        LOG.debug("consuming org bucket", gh_owner=owner_login_for_tracing)

        try:
            async with self._translate_exception_to_retries(bucket_org_key):
                sub = await subscription.Subscription.get_subscription(
                    self.redis_links.cache, owner_id
                )

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

                    # NOTE(sileht): wew don't need to refresh trains if nothing changed.
                    if pulls_processed > 0:
                        await merge_train.Train.refresh_trains(installation)

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
                await worker_lua.drop_bucket(self.redis_links.stream, bucket_org_key)
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
                    (http.HTTPServerSideError, redis_exceptions.ConnectionError),
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
            await worker_lua.clean_org_bucket(
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
        bucket_sources_key: worker_lua.BucketSourcesKeyType,
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
        bucket_org_key: worker_lua.BucketOrgKeyType,
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

            bucket_sources_key = worker_lua.BucketSourcesKeyType(
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
        bucket_org_key: worker_lua.BucketOrgKeyType,
        installation: context.Installation,
    ) -> int:
        pr_finder = pull_request_finder.PullRequestFinder(installation)

        pulls_processed = 0
        started_at = time.monotonic()
        while True:
            bucket = await self.select_pull_request_bucket(bucket_org_key)
            if bucket is None:
                break

            if (time.monotonic() - started_at) >= config.BUCKET_PROCESSING_MAX_SECONDS:
                statsd.increment(
                    "engine.buckets.preempted",
                    tags=[f"priority:{bucket.priority.name}"],
                )
                break

            pulls_processed += 1

            messages = await worker_lua.get_pull_messages(
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
                await worker_lua.remove_pull(
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
                        await worker_lua.remove_pull(
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
        pull_numbers = await pr_finder.extract_pull_numbers_from_event(
            repo_id,
            source["event_type"],
            source["data"],
        )

        # NOTE(sileht): refreshing all opened pull request because something got merged
        # has a lower priority
        if source["event_type"] == "push":
            score = worker_pusher.get_priority_score(worker_pusher.Priority.low)

        pipe = await self.redis_links.stream.pipeline()
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
        bucket_org_key: worker_lua.BucketOrgKeyType,
        installation: context.Installation,
        bucket: PullRequestBucket,
        tracing_repo_name: github_types.GitHubRepositoryNameForTracing,
        message_ids: list[T_MessageID],
        sources: list[context.T_PayloadEventSource],
    ) -> None:
        for source in sources:
            if "timestamp" in source:
                statsd.histogram(
                    "engine.buckets.events.latency",
                    (
                        date.utcnow() - date.fromisoformat(source["timestamp"])
                    ).total_seconds(),
                    tags=[
                        f"worker_id:{self.worker_id}",
                        f"priority:{bucket.priority.name}",
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
            await worker_lua.remove_pull(
                self.redis_links.stream,
                bucket_org_key,
                bucket.sources_key,
                tuple(message_ids),
            )
        except IgnoredException:
            await worker_lua.remove_pull(
                self.redis_links.stream,
                bucket_org_key,
                bucket.sources_key,
                tuple(message_ids),
            )
            logger.debug("failed to process pull request, ignoring", exc_info=True)
        except MaxPullRetry as e:
            await worker_lua.remove_pull(
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
            else:
                raise

    def should_handle_owner(
        self,
        owner_id: github_types.GitHubAccountIdType,
        dedicated_worker_owner_ids: set[github_types.GitHubAccountIdType],
        global_shared_tasks_count: int,
    ) -> bool:
        if self.dedicated_owner_id is None:
            if owner_id in dedicated_worker_owner_ids:
                return False

            return (
                Worker.get_shared_worker_id_for(owner_id, global_shared_tasks_count)
                == self.worker_id
            )
        else:
            return owner_id == self.dedicated_owner_id


# NOTE(sileht): for security reason we empty the os.environ at runtime
# We can access it only when modules load.
_DYNO = os.getenv("DYNO")


def get_process_index_from_env() -> int:
    if _DYNO:
        return int(_DYNO.rsplit(".", 1)[-1]) - 1
    else:
        return 0


def wait_before_next_retry(retry_state: tenacity.RetryCallState) -> typing.Any:
    return retry_state.next_action.__dict__["sleep"]


async def ping_redis(
    redis: redis_utils.RedisStream | redis_utils.RedisCache,
    redis_name: str,
) -> None:
    def retry_log(retry_state: tenacity.RetryCallState) -> None:
        statsd.increment("redis.client.connection.errors")
        LOG.warning(
            "Couldn't connect to Redis %s, retrying in %d seconds...",
            redis_name,
            wait_before_next_retry(retry_state),
        )

    async for attempt in tenacity.AsyncRetrying(
        wait=tenacity.wait_exponential(multiplier=0.2, max=5),
        retry=tenacity.retry_if_exception_type(redis_exceptions.ConnectionError),
        before_sleep=retry_log,
    ):
        with attempt:
            await redis.ping()


TaskRetriedForeverFuncT = abc.Callable[[], abc.Awaitable[None]]


class TaskRetriedForever(asyncio.Task[typing.Any]):
    def __init__(
        self, name: str, func: TaskRetriedForeverFuncT, sleep_time: float
    ) -> None:
        self._stopping = asyncio.Event()
        super().__init__(
            self.with_dedicated_sentry_hub(
                self.loop_and_sleep_forever(func, sleep_time)
            ),
            name=name,
        )

    def stop(self) -> None:
        if self._stopping.is_set():
            raise RuntimeError(f"Worker task `{self.get_name()}` already stopped")
        self._stopping.set()

    @staticmethod
    async def with_dedicated_sentry_hub(coro: abc.Awaitable[None]) -> None:
        with sentry_sdk.Hub(sentry_sdk.Hub.current):
            await coro

    async def loop_and_sleep_forever(
        self, func: TaskRetriedForeverFuncT, sleep_time: float
    ) -> None:
        while not self._stopping.is_set():
            try:
                await func()
            except asyncio.CancelledError:
                LOG.info("%s task killed", self.get_name())
                return
            except redis_exceptions.ConnectionError:
                statsd.increment("redis.client.connection.errors")
                LOG.warning(
                    "%s task lost Redis connection",
                    self.get_name(),
                    exc_info=True,
                )
            except Exception:
                LOG.error("%s task failed", self.get_name(), exc_info=True)

            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=sleep_time)
            except asyncio.CancelledError:
                LOG.info("%s task killed", self.get_name())
                return
            except asyncio.TimeoutError:
                pass

        LOG.debug("%s task exited", self.get_name())


WorkerServiceT = typing.Literal[
    "shared-stream",
    "dedicated-stream",
    "stream-monitoring",
    "delayed-refresh",
]
WorkerServicesT = set[WorkerServiceT]
AVAILABLE_WORKER_SERVICES = set(WorkerServiceT.__dict__["__args__"])


def asyncio_event_set_by_default() -> asyncio.Event:
    evt = asyncio.Event()
    evt.set()
    return evt


@dataclasses.dataclass
class Worker:
    idle_sleep_time: float = 0.42
    shutdown_timeout: float = config.WORKER_SHUTDOWN_TIMEOUT
    dedicated_workers_shutdown_timeout: float = 60.0
    shared_stream_tasks_per_process: int = config.SHARED_STREAM_TASKS_PER_PROCESS
    shared_stream_processes: int = config.SHARED_STREAM_PROCESSES
    dedicated_stream_processes: int = config.DEDICATED_STREAM_PROCESSES
    process_index: int = dataclasses.field(default_factory=get_process_index_from_env)
    enabled_services: WorkerServicesT = dataclasses.field(
        default_factory=lambda: AVAILABLE_WORKER_SERVICES.copy()
    )
    monitoring_idle_time: float = 60
    delayed_refresh_idle_time: float = 60
    dedicated_workers_spawner_idle_time: float = 60
    dedicated_workers_syncer_idle_time: float = 30
    retry_handled_exception_forever: bool = True

    _redis_links: redis_utils.RedisLinks = dataclasses.field(
        init=False, default_factory=lambda: redis_utils.RedisLinks(name="worker")
    )

    _stopped: asyncio.Event = dataclasses.field(
        init=False, default_factory=asyncio_event_set_by_default
    )
    _stop_task: asyncio.Task[None] | None = dataclasses.field(init=False, default=None)

    _shared_worker_tasks: list[TaskRetriedForever] = dataclasses.field(
        init=False, default_factory=list
    )
    _dedicated_worker_tasks: dict[
        github_types.GitHubAccountIdType, TaskRetriedForever
    ] = dataclasses.field(init=False, default_factory=dict)

    _stream_monitoring_task: TaskRetriedForever | None = dataclasses.field(
        init=False, default=None
    )
    _dedicated_workers_spawner_task: TaskRetriedForever | None = dataclasses.field(
        init=False, default=None
    )

    _dedicated_workers_syncer_task: TaskRetriedForever | None = dataclasses.field(
        init=False, default=None
    )

    _delayed_refresh_task: TaskRetriedForever | None = dataclasses.field(
        init=False, default=None
    )
    _owners_cache: OwnerLoginsCache = dataclasses.field(
        init=False, default_factory=OwnerLoginsCache
    )
    _dedicated_workers_owners_cache: set[
        github_types.GitHubAccountIdType
    ] = dataclasses.field(init=False, default_factory=set)

    @property
    def global_shared_tasks_count(self) -> int:
        return self.shared_stream_tasks_per_process * self.shared_stream_processes

    @staticmethod
    def extract_owner(
        bucket_org_key: worker_lua.BucketOrgKeyType,
    ) -> github_types.GitHubAccountIdType:
        return github_types.GitHubAccountIdType(int(bucket_org_key.split("~")[1]))

    async def shared_stream_worker_task(self, shared_worker_id: int) -> None:
        stream_processor = StreamProcessor(
            self._redis_links,
            worker_id=f"shared-{shared_worker_id}",
            dedicated_owner_id=None,
            owners_cache=self._owners_cache,
            retry_unhandled_exception_forever=self.retry_handled_exception_forever,
        )
        return await self._stream_worker_task(stream_processor)

    async def dedicated_stream_worker_task(
        self, owner_id: github_types.GitHubAccountIdType
    ) -> None:
        stream_processor = StreamProcessor(
            self._redis_links,
            worker_id=f"dedicated-{owner_id}",
            dedicated_owner_id=owner_id,
            owners_cache=self._owners_cache,
            retry_unhandled_exception_forever=self.retry_handled_exception_forever,
        )
        return await self._stream_worker_task(stream_processor)

    async def _get_next_bucket_to_proceed(
        self,
        stream_processor: StreamProcessor,
    ) -> worker_lua.BucketOrgKeyType | None:
        now = time.time()
        for org_bucket in await self._redis_links.stream.zrangebyscore(
            "streams", min=0, max=now
        ):
            bucket_org_key = worker_lua.BucketOrgKeyType(org_bucket.decode())
            owner_id = self.extract_owner(bucket_org_key)
            if not stream_processor.should_handle_owner(
                owner_id,
                self._dedicated_workers_owners_cache,
                self.global_shared_tasks_count,
            ):
                continue

            has_pull_requests_to_process = (
                await stream_processor.select_pull_request_bucket(bucket_org_key)
            )
            if not has_pull_requests_to_process:
                continue

            return bucket_org_key

        return None

    async def _stream_worker_task(self, stream_processor: StreamProcessor) -> None:
        logs.WORKER_ID.set(stream_processor.worker_id)

        bucket_org_key = await self._get_next_bucket_to_proceed(stream_processor)
        if bucket_org_key is None:
            return None

        LOG.debug(
            "worker %s take org bucket: %s",
            stream_processor.worker_id,
            bucket_org_key,
        )

        statsd.increment(
            "engine.streams.selected",
            tags=[f"worker_id:{stream_processor.worker_id}"],
        )

        owner_id = self.extract_owner(bucket_org_key)
        owner_login_for_tracing = self._owners_cache.get(owner_id)
        try:
            with tracer.trace(
                "org bucket processing",
                span_type="worker",
                resource=owner_login_for_tracing,
            ) as span:
                span.set_tag("gh_owner", owner_login_for_tracing)
                with sentry_sdk.Hub(sentry_sdk.Hub.current) as hub:
                    with hub.configure_scope() as scope:
                        scope.set_tag("gh_owner", owner_login_for_tracing)
                        scope.set_user({"username": owner_login_for_tracing})
                        await stream_processor.consume(
                            bucket_org_key, owner_id, owner_login_for_tracing
                        )
        finally:
            LOG.debug(
                "worker %s release org bucket: %s",
                stream_processor.worker_id,
                bucket_org_key,
            )

    @staticmethod
    async def get_dedicated_worker_owner_ids_from_redis(
        redis_stream: redis_utils.RedisStream,
    ) -> set[github_types.GitHubAccountIdType]:
        dedicated_workers_data = await redis_stream.smembers(DEDICATED_WORKERS_KEY)
        if dedicated_workers_data is None:
            return set()
        else:
            return {
                github_types.GitHubAccountIdType(int(v)) for v in dedicated_workers_data
            }

    @tracer.wrap("sync_dedicated_workers_cache", span_type="worker")
    async def _sync_dedicated_workers_cache(self) -> None:
        self._dedicated_workers_owners_cache = (
            await self.get_dedicated_worker_owner_ids_from_redis(
                self._redis_links.stream
            )
        )

    @tracer.wrap("monitoring_task", span_type="worker")
    async def monitoring_task(self) -> None:
        # TODO(sileht): maybe also graph streams that are before `now`
        # to see the diff between the backlog and the upcoming work to do
        now = time.time()
        org_buckets: list[
            tuple[bytes, float]
        ] = await self._redis_links.stream.zrangebyscore(
            "streams",
            min=0,
            max=now,
            withscores=True,
        )
        # NOTE(sileht): The latency may not be exact with the next StreamSelector
        # based on hash+modulo
        if len(org_buckets) > self.global_shared_tasks_count:
            latency = now - org_buckets[self.global_shared_tasks_count][1]
            statsd.timing("engine.streams.latency", latency)
        else:
            statsd.timing("engine.streams.latency", 0)

        statsd.gauge("engine.streams.backlog", len(org_buckets))
        statsd.gauge("engine.workers.count", self.global_shared_tasks_count)
        statsd.gauge(
            "engine.processes.count",
            self.shared_stream_processes,
            tags=["type:shared"],
        )
        statsd.gauge(
            "engine.processes.count",
            self.dedicated_stream_processes,
            tags=["type:dedicated"],
        )
        statsd.gauge(
            "engine.workers-per-process.count", self.shared_stream_tasks_per_process
        )

        # TODO(sileht): maybe we can do something with the bucket scores to
        # build a latency metric
        bucket_backlogs: dict[
            worker_pusher.Priority, dict[str, int]
        ] = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))

        for org_bucket, _ in org_buckets:
            owner_id = self.extract_owner(
                worker_lua.BucketOrgKeyType(org_bucket.decode())
            )
            if owner_id in self._dedicated_workers_owners_cache:
                worker_id = f"dedicated-{owner_id}"
            else:
                worker_id = self.get_shared_worker_id_for(
                    owner_id, self.global_shared_tasks_count
                )
            bucket_contents: list[
                tuple[bytes, float]
            ] = await self._redis_links.stream.zrangebyscore(
                org_bucket, min=0, max="+inf", withscores=True
            )
            for _, score in bucket_contents:
                prio = worker_pusher.get_priority_level_from_score(score)
                bucket_backlogs[prio][worker_id] += 1

        for priority, bucket_backlog in bucket_backlogs.items():
            for worker_id, length in bucket_backlog.items():
                statsd.gauge(
                    "engine.buckets.backlog",
                    length,
                    tags=[f"priority:{priority.name}", f"worker_id:{worker_id}"],
                )

    @tracer.wrap("delayed_refresh_task", span_type="worker")
    async def delayed_refresh_task(self) -> None:
        await delayed_refresh.send(self._redis_links)

    @staticmethod
    def get_shared_worker_id_for(
        owner_id: github_types.GitHubAccountIdType, global_shared_tasks_count: int
    ) -> str:
        hashed = hashlib.blake2s(str(owner_id).encode())
        shared_id = int(hashed.hexdigest(), 16) % global_shared_tasks_count
        return f"shared-{shared_id}"

    def get_shared_worker_ids(self) -> list[int]:
        return list(
            range(
                self.process_index * self.shared_stream_tasks_per_process,
                (self.process_index + 1) * self.shared_stream_tasks_per_process,
            )
        )

    async def start(self) -> None:
        if self._stop_task is not None:
            raise RuntimeError("Worker can't be restarted")

        self._stopped.clear()

        LOG.info(
            "worker process start",
            enabled_services=self.enabled_services,
            process_index=self.process_index,
            dedicated_stream_processes=self.dedicated_stream_processes,
            shared_stream_tasks_per_process=self.shared_stream_processes,
            shared_stream_processes=self.shared_stream_processes,
        )

        await ping_redis(self._redis_links.stream, "Stream")
        await ping_redis(self._redis_links.cache, "Cache")

        await github.GitHubAppInfo.warm_cache(self._redis_links.cache_bytes)

        self._dedicated_workers_syncer_task = TaskRetriedForever(
            "dedicated workers cache syncer",
            self._sync_dedicated_workers_cache,
            self.dedicated_workers_syncer_idle_time,
        )

        if "shared-stream" in self.enabled_services:
            worker_ids = self.get_shared_worker_ids()
            LOG.info(
                "workers starting",
                count=len(worker_ids),
                process_index=self.process_index,
                shared_stream_tasks_per_process=self.shared_stream_tasks_per_process,
            )
            for worker_id in worker_ids:
                self._shared_worker_tasks.append(
                    TaskRetriedForever(
                        f"worker {worker_id}",
                        functools.partial(
                            self.shared_stream_worker_task,
                            worker_id,
                        ),
                        self.idle_sleep_time,
                    )
                )
            LOG.info(
                "workers started",
                count=len(worker_ids),
                process_index=self.process_index,
                shared_stream_tasks_per_process=self.shared_stream_tasks_per_process,
            )

        if "dedicated-stream" in self.enabled_services:
            LOG.info("dedicated worker spawner starting")
            self._dedicated_workers_spawner_task = TaskRetriedForever(
                "dedicated workers spawner",
                self.dedicated_workers_spawner_task,
                self.dedicated_workers_spawner_idle_time,
            )
            LOG.info("dedicated worker spawner started")

        if "delayed-refresh" in self.enabled_services:
            LOG.info("delayed refresh starting")
            self._delayed_refresh_task = TaskRetriedForever(
                "delayed_refresh",
                self.delayed_refresh_task,
                self.delayed_refresh_idle_time,
            )
            LOG.info("delayed refresh started")

        if "stream-monitoring" in self.enabled_services:
            LOG.info("monitoring starting")
            self._stream_monitoring_task = TaskRetriedForever(
                "monitoring", self.monitoring_task, self.monitoring_idle_time
            )
            LOG.info("monitoring started")

    def get_my_dedicated_worker_ids_from_cache(
        self,
    ) -> set[github_types.GitHubAccountIdType]:
        if self.dedicated_stream_processes:
            return set(
                sorted(self._dedicated_workers_owners_cache)[
                    self.process_index :: self.dedicated_stream_processes
                ]
            )
        return set()

    @tracer.wrap("dedicated_workers_spawner_task", span_type="worker")
    async def dedicated_workers_spawner_task(self) -> None:
        expected_workers = self.get_my_dedicated_worker_ids_from_cache()
        current_workers = set(self._dedicated_worker_tasks.keys())

        to_stop = current_workers - expected_workers
        to_start = expected_workers - current_workers

        if to_stop:
            LOG.info(
                "dedicated workers to stop",
                workers=to_stop,
                process_index=self.process_index,
                dedicated_stream_processes=self.dedicated_stream_processes,
            )

        tasks = [self._dedicated_worker_tasks[owner_id] for owner_id in to_stop]
        if tasks:
            await self.stop_wait_and_kill(
                "old dedicated workers", tasks, self.dedicated_workers_shutdown_timeout
            )

        for owner_id in to_stop:
            del self._dedicated_worker_tasks[owner_id]

        if to_start:
            LOG.info(
                "dedicated workers to start",
                workers=to_start,
                process_index=self.process_index,
                dedicated_stream_processes=self.dedicated_stream_processes,
            )
        for owner_id in to_start:
            self._dedicated_worker_tasks[owner_id] = TaskRetriedForever(
                f"dedicated-{owner_id}",
                functools.partial(self.dedicated_stream_worker_task, owner_id),
                self.idle_sleep_time,
            )

        if to_start or to_stop:
            LOG.info(
                "new dedicated workers setup",
                workers=set(self._dedicated_worker_tasks),
                process_index=self.process_index,
                dedicated_stream_processes=self.dedicated_stream_processes,
            )

    async def _shutdown(self) -> None:
        LOG.info("shutdown start")

        # Stop spawning new workers first
        if self._dedicated_workers_spawner_task is not None:
            self._dedicated_workers_spawner_task.stop()
            await self._dedicated_workers_spawner_task

        # then we can cleanup other tasks
        tasks = []
        tasks.extend(self._shared_worker_tasks)
        tasks.extend(self._dedicated_worker_tasks.values())
        if self._dedicated_workers_syncer_task is not None:
            tasks.append(self._dedicated_workers_syncer_task)
        if self._delayed_refresh_task is not None:
            tasks.append(self._delayed_refresh_task)
        if self._stream_monitoring_task is not None:
            tasks.append(self._stream_monitoring_task)

        await self.stop_wait_and_kill(
            "workers and monitoring", tasks, self.shutdown_timeout
        )

        self._shared_worker_tasks = []
        self._dedicated_worker_tasks = {}
        self._delayed_refresh_task = None
        self._stream_monitoring_task = None
        self._dedicated_workers_spawner_task = None
        self._dedicated_workers_syncer_task = None

        LOG.info("redis finalizing")
        await self._redis_links.shutdown_all()
        LOG.info("redis finalized")

        self._stopped.set()
        LOG.info("shutdown finished")

    @staticmethod
    async def stop_wait_and_kill(
        name: str, tasks: list[TaskRetriedForever], timeout: float
    ) -> None:
        LOG.info(f"{name} exiting", count=len(tasks))
        for task in tasks:
            task.stop()
        if tasks:
            _, pendings = await asyncio.wait(tasks, timeout=timeout)
            if pendings:
                LOG.info(f"{name} being killed", count=len(pendings))
                for pending in pendings:
                    pending.cancel(msg="shutdown")
                await asyncio.wait(pendings)
        LOG.info(f"{name} exited", count=len(tasks))

    def stop(self) -> None:
        if self._stop_task is not None:
            raise RuntimeError("Worker is already stopping")
        self._stop_task = asyncio.create_task(self._shutdown(), name="shutdown")

    async def wait_shutdown_complete(self) -> None:
        await self._stopped.wait()
        if self._stop_task:
            await self._stop_task
            self._stop_task = None

    def stop_with_signal(self, signame: str) -> None:
        if self._stop_task is None:
            LOG.info("got signal %s: cleanly shutdown workers", signame)
            self.stop()
        else:
            LOG.info("got signal %s: ignoring, shutdown already in process", signame)

    def setup_signals(self) -> None:
        loop = asyncio.get_running_loop()
        for signame in ("SIGINT", "SIGTERM"):
            loop.add_signal_handler(
                getattr(signal, signame),
                functools.partial(self.stop_with_signal, signame),
            )


async def run_forever(
    enabled_services: WorkerServicesT = AVAILABLE_WORKER_SERVICES,
) -> None:
    worker = Worker(enabled_services=enabled_services)
    await worker.start()
    worker.setup_signals()
    await worker.wait_shutdown_complete()
    LOG.info("Exiting...")


def ServicesSet(v: str) -> WorkerServicesT:
    values = set(v.strip().split(","))
    for value in values:
        if value not in AVAILABLE_WORKER_SERVICES:
            raise ValueError(f"{v} is not a valid service")
    return typing.cast(WorkerServicesT, values)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Mergify Engine Worker")
    parser.add_argument(
        "--enabled-services",
        type=ServicesSet,
        default=",".join(AVAILABLE_WORKER_SERVICES),
    )
    args = parser.parse_args(argv)

    service.setup("worker")
    signals.register()
    return asyncio.run(run_forever(enabled_services=args.enabled_services))


async def async_status() -> None:
    shared_stream_tasks_per_process: int = config.SHARED_STREAM_TASKS_PER_PROCESS
    shared_stream_processes: int = config.SHARED_STREAM_PROCESSES
    global_shared_tasks_count: int = (
        shared_stream_tasks_per_process * shared_stream_processes
    )

    redis_links = redis_utils.RedisLinks(name="async_status")

    dedicated_worker_owner_ids = await Worker.get_dedicated_worker_owner_ids_from_redis(
        redis_links.stream
    )

    def sorter(item: tuple[bytes, float]) -> str:
        org_bucket, score = item
        owner_id = Worker.extract_owner(
            worker_lua.BucketOrgKeyType(org_bucket.decode())
        )
        if owner_id in dedicated_worker_owner_ids:
            return f"dedicated-{owner_id}"
        else:
            return Worker.get_shared_worker_id_for(owner_id, global_shared_tasks_count)

    org_buckets: list[tuple[bytes, float]] = sorted(
        await redis_links.stream.zrangebyscore(
            "streams", min=0, max="+inf", withscores=True
        ),
        key=sorter,
    )

    for worker_id, org_buckets_by_worker in itertools.groupby(org_buckets, key=sorter):
        for org_bucket, score in org_buckets_by_worker:
            date = datetime.datetime.utcfromtimestamp(score).isoformat(" ", "seconds")
            owner_id = org_bucket.split(b"~")[1]
            event_org_buckets = await redis_links.stream.zrange(org_bucket, 0, -1)
            count = sum([await redis_links.stream.xlen(es) for es in event_org_buckets])
            items = f"{len(event_org_buckets)} pull requests, {count} events"
            print(f"{{{worker_id}}} [{date}] {owner_id.decode()}: {items}")

    await redis_links.shutdown_all()


def status() -> None:
    asyncio.run(async_status())


async def async_reschedule_now() -> int:
    parser = argparse.ArgumentParser(description="Rescheduler for Mergify")
    parser.add_argument("owner_id", help="Organization ID")
    args = parser.parse_args()

    redis_links = redis_utils.RedisLinks(name="async_reschedule_now")
    org_buckets = await redis_links.stream.zrangebyscore("streams", min=0, max="+inf")
    expected_bucket = f"bucket~{args.owner_id}"
    for org_bucket in org_buckets:
        if org_bucket.decode().startswith(expected_bucket):
            scheduled_at = date.utcnow()
            score = scheduled_at.timestamp()
            transaction = await redis_links.stream.pipeline()
            await transaction.hdel(ATTEMPTS_KEY, org_bucket)
            # TODO(sileht): Should we update bucket scores too ?
            await transaction.zadd("streams", {org_bucket.decode(): score})
            # NOTE(sileht): Do we need to cleanup the per PR attempt?
            # await transaction.hdel(ATTEMPTS_KEY, bucket_sources_key)
            await transaction.execute()
            await redis_links.shutdown_all()
            return 0
    else:
        print(f"Stream for {expected_bucket} not found")
        await redis_links.shutdown_all()
        return 1


def reschedule_now() -> int:
    return asyncio.run(async_reschedule_now())
