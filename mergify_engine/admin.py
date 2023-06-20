import argparse
import asyncio
import typing

from mergify_engine import github_types
from mergify_engine.clients import github
from mergify_engine.clients import github_app


async def suspended(
    verb: typing.Literal["PUT", "DELETE"], owner: github_types.GitHubLogin
) -> None:
    async with github.AsyncGitHubClient(auth=github_app.GitHubBearerAuth()) as client:
        installation = typing.cast(
            github_types.GitHubInstallation,
            await client.item(f"/orgs/{owner}/installation"),
        )
        resp = await client.request(
            verb, f"/app/installations/{installation['id']}/suspended"
        )
        print(resp)


def main() -> None:
    parser = argparse.ArgumentParser(description="Mergify admin tools")
    subparsers = parser.add_subparsers(dest="command")
    suspend_parser = subparsers.add_parser("suspend", help="Suspend an installation")
    suspend_parser.add_argument("organization", help="Organization login")
    unsuspend_parser = subparsers.add_parser(
        "unsuspend", help="Unsuspend an installation"
    )
    unsuspend_parser.add_argument("organization", help="Organization login")

    args = parser.parse_args()

    try:
        if args.command == "suspend":
            asyncio.run(suspended("PUT", github_types.GitHubLogin(args.organization)))
        elif args.command == "unsuspend":
            asyncio.run(
                suspended("DELETE", github_types.GitHubLogin(args.organization))
            )
        else:
            parser.print_help()
    except KeyboardInterrupt:
        print("Interruped...")
    except BrokenPipeError:
        pass
