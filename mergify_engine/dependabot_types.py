import typing

import pydantic


DependabotAttributes = typing.TypedDict(
    "DependabotAttributes",
    {
        "dependency-name": str,
        "dependency-type": str,
        "update-type": typing.Optional[str],
    },
)

# Note(jules) update-type can be missing https://github.com/dependabot/dependabot-core/issues/4893
DependabotAttributes.__required_keys__ = frozenset(
    {"dependency-name", "dependency-type"}
)
DependabotAttributes.__optional_keys__ = frozenset({"update-type"})


class DependabotYamlMessageSchema(pydantic.BaseModel):
    updated_dependencies: list[DependabotAttributes] = pydantic.Field(
        alias="updated-dependencies", min_items=1, max_items=1
    )
