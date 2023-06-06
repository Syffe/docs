import argparse
import asyncio
import collections
import dataclasses
import datetime
import time
import typing

import daiquiri
import msgpack
import tenacity

from mergify_engine import date
from mergify_engine import exceptions
from mergify_engine import filtered_github_types
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import redis_utils
from mergify_engine import service
from mergify_engine import settings
from mergify_engine.clients import http


LOG = daiquiri.getLogger(__name__)

HOUR = datetime.timedelta(hours=1).total_seconds()
ACTIVE_USERS_PREFIX = "active-users"
ACTIVE_USERS_EVENTS_PREFIX = "active-users-events"
# NOTE(sileht): keep two months to be able answer invoice question on support
ACTIVE_USERS_EVENTS_EXPIRATION = int(datetime.timedelta(days=62).total_seconds())


ActiveUserKeyT = typing.NewType("ActiveUserKeyT", str)


# We use dataclasses with a special hash method to merge same org/repo id even
# when they are renamed
@dataclasses.dataclass(unsafe_hash=True, order=True)
class SeatAccount:
    id: github_types.GitHubAccountIdType
    login: github_types.GitHubLogin = dataclasses.field(compare=False)


@dataclasses.dataclass(unsafe_hash=True, order=True)
class ActiveUser(SeatAccount):
    seen_at: datetime.datetime = dataclasses.field(compare=False)


@dataclasses.dataclass(unsafe_hash=True, order=True)
class SeatRepository:
    id: github_types.GitHubRepositoryIdType
    name: github_types.GitHubRepositoryName = dataclasses.field(compare=False)


class SeatsCountResultT(typing.NamedTuple):
    active_users: int


class CollaboratorsSetsT(typing.TypedDict):
    active_users: set[ActiveUser] | None


CollaboratorsT = dict[
    SeatAccount,
    dict[SeatRepository, CollaboratorsSetsT],
]


def _get_active_users_key(
    repository: github_types.GitHubRepository,
) -> ActiveUserKeyT:
    return ActiveUserKeyT(
        f"{ACTIVE_USERS_PREFIX}~{repository['owner']['id']}~{repository['owner']['login']}~{repository['id']}~{repository['name']}"
    )


def _get_active_users_events_key(
    repository: github_types.GitHubRepository,
    user_id: github_types.GitHubAccountIdType,
) -> ActiveUserKeyT:
    return ActiveUserKeyT(
        f"{ACTIVE_USERS_EVENTS_PREFIX}~{repository['owner']['id']}~{repository['id']}~{user_id}"
    )


async def get_active_users_keys(
    redis: redis_utils.RedisActiveUsers,
    owner_id: typing.Literal["*"] | github_types.GitHubAccountIdType = "*",
    repo_id: (typing.Literal["*"] | github_types.GitHubRepositoryIdType) = "*",
) -> collections.abc.AsyncIterator[ActiveUserKeyT]:
    async for key in redis.scan_iter(
        f"{ACTIVE_USERS_PREFIX}~{owner_id}~*~{repo_id}~*", count=10000
    ):
        yield ActiveUserKeyT(key.decode())


def _parse_user(user: str, seen_at: datetime.datetime) -> ActiveUser:
    part1, _, part2 = user.partition("~")
    return ActiveUser(
        github_types.GitHubAccountIdType(int(part1)),
        github_types.GitHubLogin(part2),
        seen_at,
    )


async def get_active_users(
    redis: redis_utils.RedisActiveUsers, key: ActiveUserKeyT
) -> set[ActiveUser]:
    one_month_ago = datetime.datetime.utcnow() - datetime.timedelta(days=30)
    # NOTE(sileht): if two users has the same id but different names (because to change his login)
    # we got two ActiveUser with the same id, so the set will only keep the first one
    # This is why we should add to the set the most recent first.
    return {
        _parse_user(user.decode(), score)
        for user, score in reversed(
            await redis.zrangebyscore(
                key,
                min=one_month_ago.timestamp(),
                max="+inf",
                withscores=True,
                score_cast_func=lambda x: date.fromtimestamp(float(x)),
            )
        )
    }


async def store_active_users(
    redis: redis_utils.RedisActiveUsers,
    event_type: str,
    event_id: str,
    hook_id: str,
    event: github_types.GitHubEvent,
) -> None:
    typed_event: None | (
        github_types.GitHubEventPush
        | github_types.GitHubEventIssueComment
        | github_types.GitHubEventPullRequest
        | github_types.GitHubEventPullRequestReview
        | github_types.GitHubEventPullRequestReviewComment
    ) = None

    users = {}

    def _add_user(user: github_types.GitHubAccount) -> None:
        if user["id"] <= 0:
            return
        if user["login"].endswith("[bot]"):
            return
        if user["type"] == "Bot":
            return
        if user["login"] == "web-flow":
            return

        users[user["id"]] = user["login"]

    if event_type == "push":
        typed_event = typing.cast(github_types.GitHubEventPush, event)
    elif event_type == "issue_comment":
        typed_event = typing.cast(github_types.GitHubEventIssueComment, event)
        _add_user(typed_event["issue"]["user"])
        _add_user(typed_event["comment"]["user"])
    elif event_type == "pull_request":
        typed_event = typing.cast(github_types.GitHubEventPullRequest, event)
        _add_user(typed_event["pull_request"]["user"])
        list(map(_add_user, typed_event["pull_request"]["assignees"]))
    elif event_type == "pull_request_review":
        typed_event = typing.cast(github_types.GitHubEventPullRequestReview, event)
        _add_user(typed_event["pull_request"]["user"])
        list(map(_add_user, typed_event["pull_request"]["assignees"]))
        if typed_event["review"]["user"] is not None:
            _add_user(typed_event["review"]["user"])
    elif event_type == "pull_request_review_comment":
        typed_event = typing.cast(
            github_types.GitHubEventPullRequestReviewComment, event
        )
        if typed_event["pull_request"] is not None:
            _add_user(typed_event["pull_request"]["user"])
            list(map(_add_user, typed_event["pull_request"]["assignees"]))
        if typed_event["comment"] is not None:
            _add_user(typed_event["comment"]["user"])

    if typed_event is None:
        return

    _add_user(typed_event["sender"])

    if not users:
        return

    repo_key = _get_active_users_key(typed_event["repository"])
    transaction = await redis.pipeline()
    for user_id, user_login in users.items():
        user_key = f"{user_id}~{user_login}"
        await transaction.zadd(repo_key, {user_key: time.time()})
        event_key = _get_active_users_events_key(typed_event["repository"], user_id)

        await transaction.setex(
            event_key,
            ACTIVE_USERS_EVENTS_EXPIRATION,
            msgpack.packb(
                filtered_github_types.extract(event_type, event_id, hook_id, event)
            ),
        )

    await transaction.execute()


class SeatCollaboratorJsonT(typing.TypedDict):
    id: int
    login: str
    seen_at: str


class SeatCollaboratorsJsonT(typing.TypedDict):
    active_users: list[SeatCollaboratorJsonT] | None


class SeatRepositoryJsonT(typing.TypedDict):
    id: int
    name: str
    collaborators: SeatCollaboratorsJsonT


class SeatOrganizationJsonT(typing.TypedDict):
    id: int
    login: str
    repositories: list[SeatRepositoryJsonT]


class SeatsJsonT(typing.TypedDict):
    organizations: list[SeatOrganizationJsonT]


@dataclasses.dataclass
class Seats:
    seats: CollaboratorsT = dataclasses.field(
        default_factory=lambda: collections.defaultdict(
            lambda: collections.defaultdict(lambda: {"active_users": None})
        )
    )

    @classmethod
    async def get(
        cls,
        redis: redis_utils.RedisActiveUsers,
        owner_id: github_types.GitHubAccountIdType | None = None,
    ) -> "Seats":
        seats = cls()
        await seats.populate_with_active_users(redis, owner_id)
        return seats

    def jsonify(self) -> SeatsJsonT:
        data = SeatsJsonT({"organizations": []})
        for org, repos in self.seats.items():
            repos_json = []
            for repo, _seats in repos.items():
                collaborators_json = SeatCollaboratorsJsonT(
                    {
                        "active_users": (
                            None
                            if _seats["active_users"] is None
                            else [
                                {
                                    "id": seat.id,
                                    "login": seat.login,
                                    "seen_at": seat.seen_at.isoformat(),
                                }
                                for seat in _seats["active_users"]
                            ]
                        ),
                    }
                )
                repos_json.append(
                    SeatRepositoryJsonT(
                        {
                            "id": repo.id,
                            "name": repo.name,
                            "collaborators": collaborators_json,
                        }
                    )
                )
            data["organizations"].append(
                SeatOrganizationJsonT(
                    {
                        "id": org.id,
                        "login": org.login,
                        "repositories": repos_json,
                    }
                )
            )
        return data

    def count(self) -> SeatsCountResultT:
        all_active_users_collaborators = set()
        for repos in self.seats.values():
            for sets in repos.values():
                if sets["active_users"] is not None:
                    all_active_users_collaborators |= sets["active_users"]
        return SeatsCountResultT(len(all_active_users_collaborators))

    async def populate_with_active_users(
        self,
        redis: redis_utils.RedisActiveUsers,
        owner_id: github_types.GitHubAccountIdType | None = None,
    ) -> None:
        async for key in get_active_users_keys(
            redis, owner_id="*" if owner_id is None else owner_id
        ):
            _, _owner_id, owner_login, repo_id, repo_name = key.split("~")
            org = SeatAccount(
                github_types.GitHubAccountIdType(int(_owner_id)),
                github_types.GitHubLogin(owner_login),
            )
            repo = SeatRepository(
                github_types.GitHubRepositoryIdType(int(repo_id)),
                github_types.GitHubRepositoryName(repo_name),
            )
            active_users = await get_active_users(redis, key)

            repo_seats = self.seats[org][repo]
            if repo_seats["active_users"] is None:
                repo_seats["active_users"] = active_users
            else:
                repo_seats["active_users"] |= active_users


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=0.2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
async def send_seats(seats: SeatsCountResultT) -> None:
    async with http.AsyncClient() as client:
        if settings.SUBSCRIPTION_TOKEN is None:
            raise RuntimeError("SUBSCRIPTION_TOKEN is None")

        try:
            await client.post(
                f"{settings.SUBSCRIPTION_URL}/on-premise/report",
                headers={
                    "Authorization": f"token {settings.SUBSCRIPTION_TOKEN.get_secret_value()}"
                },
                json={
                    "active_users": seats.active_users,
                    "engine_version": settings.VERSION,
                },
            )
        except Exception as exc:
            if exceptions.should_be_ignored(exc):
                return
            if exceptions.need_retry(exc):
                raise tenacity.TryAgain
            raise


async def count_and_send(redis: redis_utils.RedisActiveUsers) -> None:
    await asyncio.sleep(HOUR)
    while True:
        # NOTE(sileht): We loop even if SUBSCRIPTION_TOKEN is missing to not
        # break `tox -e test`. And we can et SUBSCRIPTION_TOKEN to test the
        # daemon with `tox -etest`
        if settings.SUBSCRIPTION_TOKEN is None:
            LOG.info("on-premise subscription token missing, nothing to do.")
        else:
            try:
                seats = (await Seats.get(redis)).count()
            except Exception:
                LOG.error("failed to count seats", exc_info=True)
            else:
                try:
                    await send_seats(seats)
                except Exception:
                    LOG.error("failed to send seats usage", exc_info=True)
            LOG.info("reported seats usage", seats=seats)

        await asyncio.sleep(12 * HOUR)


async def report(args: argparse.Namespace) -> None:
    redis_links = redis_utils.RedisLinks(name="report")
    try:
        if args.daemon:
            service.setup("count-seats")
            await count_and_send(redis_links.active_users)
        else:
            service.setup("count-seats", dump_config=False)
            if settings.SUBSCRIPTION_TOKEN is None:
                LOG.error("on-premise subscription token missing")
            else:
                seats = await Seats.get(redis_links.active_users)
                if args.json:
                    print(json.dumps(seats.jsonify()))
                else:
                    seats_count = seats.count()
                    LOG.info("Active users: %s", seats_count.active_users)
    finally:
        await redis_links.shutdown_all()


def main() -> None:
    parser = argparse.ArgumentParser(description="Report used seats")
    parser.add_argument(
        "--daemon",
        "-d",
        action="store_true",
        help="Run as daemon and report usage regularly",
    )
    parser.add_argument(
        "--json",
        "-j",
        action="store_true",
        help="Output detailed usage in JSON format",
    )
    return asyncio.run(report(parser.parse_args()))
