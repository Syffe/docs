import pytest

from mergify_engine.github_types import GitHubRepositoryPermission


@pytest.mark.parametrize(
    "permission,expected_permissions",
    [
        (
            GitHubRepositoryPermission.ADMIN,
            [
                GitHubRepositoryPermission.ADMIN,
            ],
        ),
        (
            GitHubRepositoryPermission.MAINTAIN,
            [
                GitHubRepositoryPermission.MAINTAIN,
                GitHubRepositoryPermission.ADMIN,
            ],
        ),
        (
            GitHubRepositoryPermission.WRITE,
            [
                GitHubRepositoryPermission.WRITE,
                GitHubRepositoryPermission.MAINTAIN,
                GitHubRepositoryPermission.ADMIN,
            ],
        ),
        (
            GitHubRepositoryPermission.READ,
            [
                GitHubRepositoryPermission.READ,
                GitHubRepositoryPermission.WRITE,
                GitHubRepositoryPermission.MAINTAIN,
                GitHubRepositoryPermission.ADMIN,
            ],
        ),
        (
            GitHubRepositoryPermission.NONE,
            [
                GitHubRepositoryPermission.NONE,
                GitHubRepositoryPermission.READ,
                GitHubRepositoryPermission.WRITE,
                GitHubRepositoryPermission.MAINTAIN,
                GitHubRepositoryPermission.ADMIN,
            ],
        ),
    ],
)
def test_permission_above(
    permission: GitHubRepositoryPermission,
    expected_permissions: list[GitHubRepositoryPermission],
) -> None:
    actual_permissions = GitHubRepositoryPermission.permissions_above(permission)
    assert actual_permissions == expected_permissions
