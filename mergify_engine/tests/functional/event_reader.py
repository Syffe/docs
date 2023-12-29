import asyncio
import copy
import json
import time
import typing

import daiquiri
import httpx

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import utils
from mergify_engine.clients import http
from mergify_engine.tests.functional import utils as tests_utils


LOG = daiquiri.getLogger(__name__)

FAKE_DATA = "whatdataisthat"
FAKE_HMAC = utils.compute_hmac(
    FAKE_DATA.encode("utf8"),
    settings.GITHUB_WEBHOOK_SECRET.get_secret_value(),
)


class MissingEventTimeout(Exception):
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


class EventReader:
    def __init__(
        self,
        app: httpx.AsyncClient,
        integration_id: int,
        repository_id: github_types.GitHubRepositoryIdType,
        test_name: str,
    ) -> None:
        self._app = app
        self._session = http.AsyncClient()
        self._handled_events: asyncio.Queue[ForwardedEvent] = asyncio.Queue()
        self._counter = 0

        hostname = settings.GITHUB_URL.host
        self.base_event_forwarder_url = f"{settings.TESTING_FORWARDER_ENDPOINT}/events/{hostname}/{integration_id}/{repository_id}/"
        self.test_name = test_name.replace("/", "-")

    def get_events_forwarder_url(self, test_id: str | None = None) -> str:
        if test_id is None:
            test_id = self.test_name

        return f"{self.base_event_forwarder_url}{test_id}"

    async def aclose(self) -> None:
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

    EVENTS_POLLING_INTERVAL_SECONDS = 0.20 if settings.TESTING_RECORD else 0
    EVENTS_WAITING_TIME_SECONDS = (
        settings.TESTING_RECORD_EVENTS_WAITING_TIME if settings.TESTING_RECORD else 2
    )

    async def wait_for(
        self,
        event_type: github_types.GitHubEventType,
        expected_payload: typing.Any,
        forward_to_engine: bool = True,
        test_id: str | None = None,
    ) -> github_types.GitHubEvent:
        return (
            await self.wait_for_all(
                [
                    {
                        "event_type": event_type,
                        "payload": expected_payload,
                        "test_id": test_id,
                    },
                ],
                forward_to_engine,
            )
        )[0].event

    async def wait_for_all(
        self,
        expected_events: list[WaitForAllEvent],
        forward_to_engine: bool = True,
    ) -> list[EventReceived]:
        test_ids: set[str | None] = set()
        for event_data in expected_events:
            LOG.log(
                42,
                "WAITING FOR %s/%s: %s",
                event_data["event_type"],
                event_data["payload"].get("action"),
                event_data["payload"],
            )
            test_ids.add(event_data.get("test_id"))

        # NOTE(Kontrolix): Copy events to not alter the orignal list
        expected_events = list(expected_events)

        received_events = []

        started_at = time.monotonic()
        while time.monotonic() - started_at < self.EVENTS_WAITING_TIME_SECONDS:
            if not expected_events:
                break

            try:
                event = self._handled_events.get_nowait()
                await self._process_event(event, forward_to_engine)
            except asyncio.QueueEmpty:
                found_events = False
                for test_id in test_ids:
                    for event in await self._get_events(test_id=test_id):
                        found_events = True
                        await self._handled_events.put(event)

                if not found_events:
                    await asyncio.sleep(self.EVENTS_POLLING_INTERVAL_SECONDS)

                continue

            for expected_event_data in expected_events:
                if event["type"] == expected_event_data[
                    "event_type"
                ] and tests_utils.match_expected_data(
                    event["payload"],
                    expected_event_data["payload"],
                ):
                    received_events.append(
                        EventReceived(event_type=event["type"], event=event["payload"]),
                    )
                    expected_events.remove(expected_event_data)
                    # NOTE(Kontrolix): Restart timer every time we receive an
                    # expected event
                    started_at = time.monotonic()

                    if expected_events:
                        # Reconstruct the set to not query useless test_ids
                        test_ids = {d.get("test_id") for d in expected_events}

                    break

        if expected_events:
            raise MissingEventTimeout(
                event_type=expected_events[0]["event_type"],
                expected_payload=expected_events[0]["payload"],
            )

        return received_events

    async def _get_events(self, test_id: str | None = None) -> list[ForwardedEvent]:
        # NOTE(sileht): we use a counter to make each call unique in cassettes
        self._counter += 1
        return typing.cast(
            list[ForwardedEvent],
            (
                await self._session.request(
                    "GET",
                    f"{self.get_events_forwarder_url(test_id)}?counter={self._counter}",
                    content=FAKE_DATA,
                    headers={"X-Hub-Signature": "sha1=" + FAKE_HMAC},
                )
            ).json(),
        )

    async def _process_event(self, event: typing.Any, forward_to_engine: bool) -> None:
        payload = event["payload"]
        if event["type"] in ["check_run", "check_suite"]:
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
        if forward_to_engine:
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
