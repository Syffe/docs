#!/usr/bin/env python3

import re
from urllib import request


VERSIONS_RE = re.compile(r"href=[\"']datadog_mergify-([\d\.]+)-py3-none-any.whl")


def get_latest_version_number() -> str:
    index_html_request = request.urlopen(
        "https://dd-integrations-core-wheels-build-stable.datadoghq.com/targets/simple/datadog-mergify/index.html",
    )
    response = index_html_request.read().decode()
    matchs = VERSIONS_RE.findall(response)
    if not matchs:
        raise Exception(  # noqa
            "No versions found in request of datadog-mergify versions file",
        )

    matchs.sort(key=lambda s: list(map(int, s.split("."))))
    return matchs[-1]


if __name__ == "__main__":
    print(get_latest_version_number())
