#!/usr/bin/env python3
# ruff: noqa: T201

import json
import pathlib
import re
from urllib import request


RUNTIME_TXT_FILE = pathlib.Path(__file__).parent / ".." / "runtime.txt"


def get_runtime_txt_version() -> str:
    with RUNTIME_TXT_FILE.open() as f:
        version_raw = f.read()

    return version_raw.replace("python-", "")


def version_str_to_version_int_list(s: str) -> list[int]:
    return list(map(int, s.split(".")))


def get_latest_version_number() -> str:
    github_action_request = request.urlopen(
        "https://raw.githubusercontent.com/actions/python-versions/main/versions-manifest.json",
    )
    github_action_response = json.load(github_action_request)
    github_action_tags: set[str] = {tag["version"] for tag in github_action_response}

    docker_request = request.urlopen(
        "https://hub.docker.com/v2/repositories/library/python/tags/?name=-slim&page_size=50",
    )
    docker_response = json.load(docker_request)["results"]
    docker_tags: set[str] = {
        tag["name"].replace("-slim", "")
        for tag in docker_response
        if re.fullmatch(r"^\d+\.\d+\.\d+-slim", tag["name"])
    }

    common_tags = list(docker_tags & github_action_tags)
    common_tags.sort(key=version_str_to_version_int_list)

    runtime_version = get_runtime_txt_version()
    if version_str_to_version_int_list(
        common_tags[-1],
    ) > version_str_to_version_int_list(runtime_version):
        return common_tags[-1]

    return runtime_version


if __name__ == "__main__":
    print(get_latest_version_number())
