import argparse
import asyncio
import secrets
import typing

from mergify_engine import config
from mergify_engine import utils
from mergify_engine.clients import http


async def api_call(*args: typing.Any, **kwargs: typing.Any) -> None:
    async with http.AsyncClient() as client:
        r = await client.request(*args, **kwargs)
    r.raise_for_status()
    print(r.text)


def clear_token_cache() -> None:
    parser = argparse.ArgumentParser(description="Force refresh of installation token")
    parser.add_argument("owner_id")
    args = parser.parse_args()
    asyncio.run(
        api_call(
            "DELETE",
            config.BASE_URL + f"/subscription-cache/{args.owner_id}",
            headers={"Authorization": f"bearer {config.DASHBOARD_TO_ENGINE_API_KEY}"},
        )
    )


def refresher() -> None:
    parser = argparse.ArgumentParser(description="Force refresh of mergify_engine")
    parser.add_argument(
        "--action", default="user", choices=["user", "admin", "internal"]
    )
    parser.add_argument(
        "urls",
        nargs="*",
        help=(
            "<owner>/<repo>, <owner>/<repo>/branch/<branch>, "
            "<owner>/<repo>/pull/<pull#> or "
            "https://github.com/<owner>/<repo>/pull/<pull#>"
        ),
    )

    args = parser.parse_args()

    if args.urls:
        for url in args.urls:
            url = url.replace("https://github.com/", "")
            data = secrets.token_hex(250)
            hmac = utils.compute_hmac(data.encode(), config.WEBHOOK_SECRET)
            asyncio.run(
                api_call(
                    "POST",
                    f"{config.BASE_URL}/refresh/{url}?action={args.action}",
                    headers={"X-Hub-Signature": "sha1=" + hmac},
                    content=data,
                )
            )
    else:
        parser.print_help()
