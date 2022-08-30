# -*- encoding: utf-8 -*-
#
# Copyright © 2022 Mergify SAS
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

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
            GitHubRepositoryPermission.WRITE,
            [
                GitHubRepositoryPermission.WRITE,
                GitHubRepositoryPermission.ADMIN,
            ],
        ),
        (
            GitHubRepositoryPermission.READ,
            [
                GitHubRepositoryPermission.READ,
                GitHubRepositoryPermission.WRITE,
                GitHubRepositoryPermission.ADMIN,
            ],
        ),
        (
            GitHubRepositoryPermission.NONE,
            [
                GitHubRepositoryPermission.NONE,
                GitHubRepositoryPermission.READ,
                GitHubRepositoryPermission.WRITE,
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
