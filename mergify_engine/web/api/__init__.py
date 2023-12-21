import typing


default_responses: dict[int | str, dict[str, typing.Any]] | None = {
    403: {"description": "Forbidden"},
}
