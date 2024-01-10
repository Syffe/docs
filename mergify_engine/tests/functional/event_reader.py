from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import re
import time
import typing

import daiquiri

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import utils
from mergify_engine.clients import http
from mergify_engine.tests.functional import utils as tests_utils


if typing.TYPE_CHECKING:
    import httpx
    import vcr.cassette

LOG = daiquiri.getLogger(__name__)

FAKE_DATA = "whatdataisthat"
FAKE_HMAC = utils.compute_hmac(
    FAKE_DATA.encode("utf8"),
    settings.GITHUB_WEBHOOK_SECRET.get_secret_value(),
)


class MissingEventTimeoutError(Exception):
    def __init__(
        self,
        event_type: github_types.GitHubEventType,
        expected_payload: typing.Any,
    ) -> None:
        super().__init__(
            f"Never got event `{event_type}` with payload `{expected_payload}` (timeout)",
        )


class ForwardedEvent(typing.TypedDict):
    payload: github_types.GitHubEvent
    type: github_types.GitHubEventType
    id: str


class EventReceived(typing.NamedTuple):
    event_type: github_types.GitHubEventType
    event: github_types.GitHubEvent


class WaitForAllEvent(typing.TypedDict):
    event_type: github_types.GitHubEventType
    payload: typing.Any
    test_id: typing.NotRequired[str | None]


def _get_test_ids_from_expected_events(
    expected_events: list[WaitForAllEvent],
) -> list[str | None]:
    # Use a list because set are unordered, which could lead to
    # the iteration not being the same between record and replay.
    test_ids: list[str | None] = []
    for event_data in expected_events:
        if event_data.get("test_id") not in test_ids:
            test_ids.append(event_data.get("test_id"))

    return test_ids


class EventReader:
    EVENTS_POLLING_INTERVAL_SECONDS = 0.20 if settings.TESTING_RECORD else 0
    EVENTS_WAITING_TIME_SECONDS = (
        settings.TESTING_RECORD_EVENTS_WAITING_TIME if settings.TESTING_RECORD else 2
    )

    def __init__(
        self,
        app: httpx.AsyncClient,
        vcr_cassette: vcr.cassette.Cassette,
        integration_id: int,
        repository_id: github_types.GitHubRepositoryIdType,
        test_name: str,
    ) -> None:
        self._app = app
        self._vcr_cassette = vcr_cassette
        self._session = http.AsyncClient()

        self._received_events: asyncio.Queue[ForwardedEvent] = asyncio.Queue()
        self.test_ids: set[str | None] = {None}
        self.stop_fetching_events = False
        self.forward_events_to_engine = True
        self._counter = 0
        self._breakpoint_counter = 0
        self.is_processing_events = False

        self.base_event_forwarder_url = f"{settings.TESTING_FORWARDER_ENDPOINT}/events/{settings.GITHUB_URL.host}/{integration_id}/{repository_id}/"
        self.regex_event_forwarder_req = re.compile(
            rf"^{self.base_event_forwarder_url}.*\?counter=(\d+)$",
        )
        self.test_name = test_name.replace("/", "-")

        # The index is the _counter of the requests
        self.event_forwarder_requests: dict[int, tuple[int, str]] = {}
        self.max_counter_replay = -1

        if not settings.TESTING_RECORD:
            _vcr_cassette_requests = self._vcr_cassette.requests
            for idx, request in enumerate(_vcr_cassette_requests):
                match_efwdr = self.regex_event_forwarder_req.match(request.uri)
                if match_efwdr:
                    self.event_forwarder_requests[int(match_efwdr.group(1))] = (
                        idx,
                        request.uri,
                    )

            if self.event_forwarder_requests:
                self.max_counter_replay = max(self.event_forwarder_requests.keys())

    def add_test_id(self, test_id: str) -> None:
        self.test_ids.add(test_id)

    def remove_test_id(self, test_id: str) -> None:
        assert test_id is not None, "Cannot remove `None` from EventReader's `test_ids`"
        self.test_ids -= {test_id}

    def get_events_forwarder_url(self, test_id: str | None = None) -> str:
        if test_id is None:
            test_id = self.test_name

        return f"{self.base_event_forwarder_url}{test_id}"

    async def aclose(self) -> None:
        self.stop_fetching_events = True
        self.test_ids = {None}
        self._received_events = asyncio.Queue()
        await self.drain()
        await self._session.aclose()

    async def drain(self) -> None:
        # NOTE(sileht): Drop any pending events still on the server
        r = await self._session.request(
            "DELETE",
            self.get_events_forwarder_url(),
            content=FAKE_DATA,
            headers={"X-Hub-Signature": "sha1=" + FAKE_HMAC},
        )
        r.raise_for_status()

    @contextlib.asynccontextmanager
    async def set_forwarding_events_status(
        self,
        forward: bool,
    ) -> typing.AsyncGenerator[None, None]:
        # We put a breakpoint before and after the value of
        # `self.forward_events_to_engine` changed in order for events,
        # in replay mode, to not be replayed too early and be wrongfully
        # forwarded, or not, to the engine.
        backup = self.forward_events_to_engine
        self.forward_events_to_engine = forward
        if backup != forward:
            await self.put_event_breakpoint(
                f"set forward_events_to_engine to {forward}",
            )

        yield

        self.forward_events_to_engine = backup
        if backup != forward:
            await self.put_event_breakpoint(
                f"restore forward_events_to_engine to {backup}",
            )

    async def put_event_breakpoint(self, breakpoint_name: str) -> None:
        self._breakpoint_counter += 1

        LOG.info("#" * 50)
        LOG.info(
            "breakpoint %s / %s",
            self._breakpoint_counter,
            breakpoint_name,
        )

        await self._app.put(
            f"/__breakpoint?counter={self._breakpoint_counter}",
            headers={"X-Breakpoint-Name-X": breakpoint_name},
        )

    async def wait_for(
        self,
        event_type: github_types.GitHubEventType,
        expected_payload: typing.Any,
    ) -> github_types.GitHubEvent:
        events = await self.wait_for_all(
            [
                {
                    "event_type": event_type,
                    "payload": expected_payload,
                },
            ],
        )
        return events[0].event

    async def wait_for_all(
        self,
        expected_events: list[WaitForAllEvent],
    ) -> list[EventReceived]:
        for event_data in expected_events:
            LOG.log(
                42,
                "WAITING FOR %s/%s: %s",
                event_data["event_type"],
                event_data["payload"].get("action"),
                event_data["payload"],
            )

        # NOTE(Kontrolix): Copy events to not alter the orignal list
        remaining_expected_events = list(expected_events)
        received_events = []

        def handle_received_event(event: ForwardedEvent) -> bool:
            for expected_event_data in remaining_expected_events:
                if event["type"] == expected_event_data[
                    "event_type"
                ] and tests_utils.match_expected_data(
                    event["payload"],
                    expected_event_data["payload"],
                ):
                    LOG.log(
                        42,
                        "FOUND EVENT `%s/%s: %s`",
                        expected_event_data["event_type"],
                        expected_event_data["payload"].get("action"),
                        expected_event_data["payload"],
                    )

                    received_events.append(
                        EventReceived(
                            event_type=event["type"],
                            event=event["payload"],
                        ),
                    )
                    remaining_expected_events.remove(expected_event_data)
                    return True

            return False

        started_at = time.monotonic()
        while (
            time.monotonic() - started_at < self.EVENTS_WAITING_TIME_SECONDS
            and remaining_expected_events
        ):
            try:
                event = await asyncio.wait_for(
                    self._received_events.get(),
                    timeout=self.EVENTS_WAITING_TIME_SECONDS
                    - (time.monotonic() - started_at),
                )
            except (TimeoutError, asyncio.QueueEmpty):
                raise MissingEventTimeoutError(
                    event_type=remaining_expected_events[0]["event_type"],
                    expected_payload=remaining_expected_events[0]["payload"],
                )

            if handle_received_event(event):
                started_at = time.monotonic()

            if not remaining_expected_events:
                break

            # asyncio.sleep in order to give the hand back to the
            # task that retrieves event
            await asyncio.sleep(self.EVENTS_POLLING_INTERVAL_SECONDS)

        if remaining_expected_events:
            raise MissingEventTimeoutError(
                event_type=expected_events[0]["event_type"],
                expected_payload=expected_events[0]["payload"],
            )

        return received_events

    def has_requests_to_replay(self) -> bool:
        if settings.TESTING_RECORD:
            return False

        if self._counter >= self.max_counter_replay:
            return False

        cassette_req_idx = self.event_forwarder_requests[self._counter + 1][0]
        idx_previous_request = cassette_req_idx - 1
        return bool(self._vcr_cassette.play_counts[idx_previous_request] > 0)

    def can_replay_next_request(self) -> bool:
        return not self.is_processing_events and self.has_requests_to_replay()

    async def replay_next_requests(self) -> None:
        while self.can_replay_next_request():
            events = await self._get_events(
                from_uri=self.event_forwarder_requests[self._counter + 1][1],
            )
            await self._store_and_process_events(events)

    async def _store_and_process_events(self, events: list[ForwardedEvent]) -> None:
        # This variable is used by `FunctionalTestBase.run_engine` and
        # `can_replay_next_request`. It allows the engine workers to not be launched
        # before every events currently received were forwarded, and allow the replay
        # mode to not replay another request if it is already processing events
        # from a previous request.
        self.is_processing_events = True

        for event in events:
            await self._process_event(event)
            self._received_events.put_nowait(event)

        self.is_processing_events = False

    async def receive_and_forward_events_to_engine(self) -> Exception | None:
        while not self.stop_fetching_events:
            try:
                await self._fetch_events_record_mode()
            except Exception as e:
                if (
                    isinstance(e, RuntimeError)
                    and str(e)
                    == "Cannot send a request, as the client has been closed."
                ):
                    # The test failed/timed-out. Gracefully exit in order
                    # to not get this useless error in the backtrace.
                    break

                LOG.critical(
                    "Got an exception in EventReader asyncio task: %s",
                    str(e),
                )
                # Return the exception instead of raising it, in order for
                # the error to be displayed as if it was thrown by the test itself.
                # If we let the exception be raised here (in the asyncio task),
                # the formatting of it is weird and hard to read.
                return e

            await asyncio.sleep(self.EVENTS_POLLING_INTERVAL_SECONDS)

        return None

    async def _fetch_events_record_mode(self) -> None:
        # Iterate on a copy of the test_ids to not get an exception if
        # it change size when iterating over it.
        for test_id in self.test_ids.copy():
            events = await self._get_events(test_id=test_id)
            await self._store_and_process_events(events)

    async def _get_events(
        self,
        test_id: str | None = None,
        from_uri: str | None = None,
    ) -> list[ForwardedEvent]:
        # NOTE(sileht): we use a counter to make each call unique in cassettes
        self._counter += 1

        resp = await self._session.request(
            "GET",
            from_uri
            or f"{self.get_events_forwarder_url(test_id)}?counter={self._counter}",
            content=FAKE_DATA,
            headers={"X-Hub-Signature": "sha1=" + FAKE_HMAC},
        )

        # Decrease the counter because we do not persist empty event forwarder requests to the cassette.
        # Linked to the mock `mocked__record_responses` in FunctionalTestBase
        if not resp.json():
            self._counter -= 1

        return typing.cast(list[ForwardedEvent], resp.json())

    async def _process_event(self, event: typing.Any) -> None:
        payload = event["payload"]
        if event["type"] in {"check_run", "check_suite"}:
            extra = (
                f"/{payload[event['type']].get('status')}"
                f"/{payload[event['type']].get('conclusion')}"
            )
        elif event["type"] == "status":
            extra = f"/{payload.get('state')}"
        else:
            extra = ""

        LOG.log(
            42,
            "EVENT RECEIVED %s/%s%s: %s",
            event["type"],
            payload.get("action"),
            extra,
            tests_utils.remove_useless_links(copy.deepcopy(event)),
        )
        if self.forward_events_to_engine:
            await self._app.post(
                "/event",
                headers={
                    "X-GitHub-Event": event["type"],
                    "X-GitHub-Delivery": "123456789",
                    "X-Hub-Signature": "sha1=whatever",
                    "Content-type": "application/json",
                },
                content=json.dumps(payload),
            )
