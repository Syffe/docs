import pytest

from mergify_engine import github_types
from mergify_engine.ci import models as ci_models


@pytest.mark.parametrize(
    "job_payload,expected_os,expected_cores",
    (
        ({"labels": ["ubuntu-20.04"]}, "Linux", 2),
        ({"labels": ["ubuntu-latest"]}, "Linux", 2),
        ({"labels": ["ubuntu-latest-8-cores"]}, "Linux", 8),
        ({"labels": ["windows-latest"]}, "Windows", 2),
        ({"labels": ["windows-2022"]}, "Windows", 2),
        ({"labels": ["windows-latest-64-cores"]}, "Windows", 64),
        ({"labels": ["macos-latest"]}, "macOS", 3),
        ({"labels": ["macos-12"]}, "macOS", 3),
        ({"labels": ["macos-latest-xl"]}, "macOS", 12),
        ({"labels": ["macos-12-xl"]}, "macOS", 12),
        ({"labels": ["macos-10.15"]}, "macOS", 3),
        ({"labels": []}, "Unknown", 0),
        ({"labels": ["whatever-latest"]}, "Unknown", 0),
    ),
)
async def test_extract_runner_properties(
    job_payload: github_types.GitHubJobRun,
    expected_os: ci_models.OperatingSystem,
    expected_cores: int,
) -> None:
    runner_properties = ci_models.JobRun._extract_runner_properties(job_payload)

    assert runner_properties.operating_system == expected_os
    assert runner_properties.cores == expected_cores
