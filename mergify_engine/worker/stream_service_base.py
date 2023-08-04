import dataclasses
import time

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
from ddtrace import tracer
import sentry_sdk

from mergify_engine import github_types
from mergify_engine import logs
from mergify_engine.worker import stream
from mergify_engine.worker import stream_lua
from mergify_engine.worker import task


LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class StreamService(task.SimpleService):
    retry_handled_exception_forever: bool

    _owners_cache: stream.OwnerLoginsCache = dataclasses.field(
        init=False, default_factory=stream.OwnerLoginsCache
    )

    def should_handle_owner(
        self,
        stream_processor: stream.Processor,
        owner_id: github_types.GitHubAccountIdType,
    ) -> bool:
        raise NotImplementedError

    @staticmethod
    def extract_owner(
        bucket_org_key: stream_lua.BucketOrgKeyType,
    ) -> github_types.GitHubAccountIdType:
        return github_types.GitHubAccountIdType(int(bucket_org_key.split("~")[1]))

    async def _get_next_bucket_to_proceed(
        self,
        stream_processor: stream.Processor,
    ) -> stream_lua.BucketOrgKeyType | None:
        now = time.time()
        for org_bucket in await self.redis_links.stream.zrangebyscore(
            "streams", min=0, max=now
        ):
            bucket_org_key = stream_lua.BucketOrgKeyType(org_bucket.decode())
            owner_id = self.extract_owner(bucket_org_key)
            if not self.should_handle_owner(stream_processor, owner_id):
                continue

            has_pull_requests_to_process = (
                await stream_processor.select_pull_request_bucket(bucket_org_key)
            )
            if not has_pull_requests_to_process:
                continue

            return bucket_org_key

        return None

    async def _stream_worker_task(self, stream_processor: stream.Processor) -> None:
        logs.WORKER_ID.set(stream_processor.worker_id)

        bucket_org_key = await self._get_next_bucket_to_proceed(stream_processor)
        if bucket_org_key is None:
            return

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
