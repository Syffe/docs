import argparse
import asyncio
import json
import logging
import os

from mergify_engine import config
from mergify_engine import logs
from mergify_engine import settings
from mergify_engine import utils
from mergify_engine.clients import http


LOG = logging.getLogger(__name__)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--repository-id", default=config.TESTING_REPOSITORY_ID)
    parser.add_argument("--dest", default="http://localhost:8802/event")

    args = parser.parse_args()

    logs.setup_logging()

    payload_data = os.urandom(250)
    payload_hmac = utils.compute_hmac(
        payload_data, settings.GITHUB_WEBHOOK_SECRET.get_secret_value()
    )

    async with http.AsyncClient(
        base_url="https://test-forwarder.mergify.com",
        headers={"X-Hub-Signature": "sha1=" + payload_hmac},
    ) as session:
        url = f"/events/github.com/{config.INTEGRATION_ID}/{args.repository_id}"
        if args.clean:
            r = await session.request("DELETE", url, content=payload_data)
            r.raise_for_status()

        while True:
            try:
                resp = await session.request("GET", url, content=payload_data)
                events = resp.json()
                for event in reversed(events):
                    LOG.info("")
                    LOG.info("==================================================")
                    LOG.info(
                        ">>> GOT EVENT: %s %s/%s",
                        event["id"],
                        event["type"],
                        event["payload"].get("state", event["payload"].get("action")),
                    )
                    data = json.dumps(event["payload"])
                    hmac = utils.compute_hmac(
                        data.encode("utf8"),
                        settings.GITHUB_WEBHOOK_SECRET.get_secret_value(),
                    )
                    await session.post(
                        args.dest,
                        headers={
                            "X-GitHub-Event": event["type"],
                            "X-GitHub-Delivery": event["id"],
                            "X-Hub-Signature": f"sha1={hmac}",
                            "Content-type": "application/json",
                        },
                        content=data,
                    )
            except Exception:
                LOG.error("event handling failure", exc_info=True)
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
