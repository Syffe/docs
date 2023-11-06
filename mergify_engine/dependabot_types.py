import pydantic
import typing_extensions


DependabotAttributes = typing_extensions.TypedDict(
    "DependabotAttributes",
    {
        "dependency-name": str,
        "dependency-type": str,
        "update-type": typing_extensions.NotRequired[str],
    },
)


class DependabotYamlMessageSchema(pydantic.BaseModel):
    updated_dependencies: list[DependabotAttributes] = pydantic.Field(
        alias="updated-dependencies", min_length=1
    )
