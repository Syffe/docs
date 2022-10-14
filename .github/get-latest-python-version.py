#!/usr/bin/env python3

import json
import re
import urllib.request as request


def get_latest_version_number() -> str:
    github_action_request = request.urlopen(
        "https://raw.githubusercontent.com/actions/python-versions/main/versions-manifest.json"
    )
    docker_request = request.urlopen(
        "https://hub.docker.com/v2/repositories/library/python/tags/?name=-slim&page_size=50"
    )

    github_action_tags = json.load(github_action_request)
    github_action_tags = {tag["version"] for tag in github_action_tags}

    docker_tags = json.load(docker_request)["results"]
    docker_tags = {
        tag["name"].replace("-slim", "")
        for tag in docker_tags
        if re.fullmatch(r"^\d+\.\d+\.\d+-slim", tag["name"])
    }

    common_tags = list(docker_tags & github_action_tags)
    common_tags.sort(key=lambda s: list(map(int, s.split("."))))
    return common_tags[-1]


if __name__ == "__main__":
    print(get_latest_version_number())
