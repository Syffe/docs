import pydantic
import typing_extensions


DependabotAttributes = typing_extensions.TypedDict(
    "DependabotAttributes",
    {
        "dependency-name": str,
        "dependency-type": str,
        "update-type": str | None,
    },
)

# Note(jules) update-type can be missing https://github.com/dependabot/dependabot-core/issues/4893
DependabotAttributes.__required_keys__ = frozenset(
    {"dependency-name", "dependency-type"}
)
DependabotAttributes.__optional_keys__ = frozenset({"update-type"})


class DependabotYamlMessageSchema(pydantic.BaseModel):
    updated_dependencies: list[DependabotAttributes] = pydantic.Field(
        alias="updated-dependencies", min_length=1, max_length=1
    )
