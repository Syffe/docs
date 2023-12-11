# ruff: noqa: T201
import os
import sys
from urllib import parse


def main() -> None:
    # NOTE(sileht): we pass the url via env to ensure it can't leak in CI logs
    url = os.environ[sys.argv[1]]
    result = parse.urlparse(url)
    assert result.username is not None
    assert result.password is not None
    print(
        f"""
export PGHOST="{result.hostname}"
export PGPORT="{result.port}"
export PGDATABASE="{result.path[1:]}"
export PGUSER="{parse.unquote(result.username)}"
export PGPASSWORD="{parse.unquote(result.password)}"
    """,
    )


if __name__ == "__main__":
    main()
