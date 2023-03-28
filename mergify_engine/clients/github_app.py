from collections import abc
import dataclasses
import os
import threading
import time
import typing

import daiquiri
import httpx
import jwt

from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import settings


LOG = daiquiri.getLogger(__name__)

EXPECTED_MINIMAL_PERMISSIONS: dict[
    github_types.GitHubAccountType, github_types.GitHubInstallationPermissions
] = {
    "Organization": {
        "checks": "write",
        "contents": "write",
        "issues": "write",
        "metadata": "read",
        "pages": "write",
        "pull_requests": "write",
        "statuses": "read",
        "members": "read",
    },
    "User": {
        "checks": "write",
        "contents": "write",
        "issues": "write",
        "metadata": "read",
        "pages": "write",
        "pull_requests": "write",
        "statuses": "read",
    },
}


if os.getenv("MERGIFYENGINE_TEST_SETTINGS") is not None:
    # NOTE(sileht): Here the permission that's differ from testing app and production app
    EXPECTED_MINIMAL_PERMISSIONS["User"]["statuses"] = "write"
    EXPECTED_MINIMAL_PERMISSIONS["Organization"]["statuses"] = "write"


@dataclasses.dataclass
class JwtHandler:
    jwt: str | None = None
    jwt_expiration: float | None = None
    lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)

    JWT_EXPIRATION: typing.ClassVar[int] = 60

    def get_or_create(self, force: bool = False) -> str:
        now = int(time.time())
        with self.lock:
            if (
                force
                or self.jwt is None
                or self.jwt_expiration is None
                or self.jwt_expiration <= now
            ):
                self.jwt_expiration = now + self.JWT_EXPIRATION
                payload = {
                    "iat": now,
                    "exp": self.jwt_expiration,
                    "iss": settings.GITHUB_APP_ID,
                }
                self.jwt = jwt.encode(
                    payload,
                    key=settings.GITHUB_PRIVATE_KEY.get_secret_value(),
                    algorithm="RS256",
                )
                LOG.debug("New JWT created", expire_at=self.jwt_expiration)
        return self.jwt


get_or_create_jwt = JwtHandler().get_or_create


def permissions_need_to_be_updated(
    installation: github_types.GitHubInstallation,
) -> bool:
    expected_permissions = EXPECTED_MINIMAL_PERMISSIONS[installation["target_type"]]
    for perm_name, perm_level in expected_permissions.items():
        if installation["permissions"].get(perm_name) != perm_level:
            LOG.debug(
                "The Mergify installation doesn't have the required permissions",
                gh_owner=installation["account"]["login"],
                permissions=installation["permissions"],
            )
            # FIXME(sileht): Looks like ton of people have not all permissions
            # Or this is buggy, so disable it for now.
            if perm_name in ["checks", "pull_requests", "contents"]:
                raise exceptions.MergifyNotInstalled()
            return True
    return False


class GithubBearerAuth(httpx.Auth):
    def auth_flow(
        self, request: httpx.Request
    ) -> abc.Generator[httpx.Request, httpx.Response, None]:
        bearer = get_or_create_jwt()
        request.headers["Authorization"] = f"Bearer {bearer}"
        response = yield request
        if response.status_code == 401:
            bearer = get_or_create_jwt(force=True)
            request.headers["Authorization"] = f"Bearer {bearer}"
            yield request
