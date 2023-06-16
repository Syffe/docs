import dataclasses
import typing

import pydantic


CheckStateT = typing.Literal[
    "success",
    "failure",
    "error",
    "cancelled",
    "skipped",
    "action_required",
    "timed_out",
    "pending",
    "neutral",
    "stale",
]

# Lower value will be displayed first
CHECK_SORTING: dict[CheckStateT | None, int] = {
    "failure": 0,
    "error": 0,
    "cancelled": 0,
    "action_required": 0,
    "timed_out": 0,
    "pending": 1,
    None: 1,
    "neutral": 2,
    "skipped": 2,
    "stale": 2,
    "success": 100,
}


@pydantic.dataclasses.dataclass
class QueueCheck:
    name: str = dataclasses.field(metadata={"description": "Check name"})
    description: str = dataclasses.field(metadata={"description": "Check description"})
    url: str | None = dataclasses.field(metadata={"description": "Check detail url"})
    state: CheckStateT = dataclasses.field(metadata={"description": "Check state"})
    avatar_url: str | None = dataclasses.field(
        metadata={"description": "Check avatar_url"}
    )

    class Serialized(typing.TypedDict):
        name: str
        description: str
        url: str | None
        state: CheckStateT
        avatar_url: str | None

    def serialized(self) -> "QueueCheck.Serialized":
        return self.Serialized(
            name=self.name,
            description=self.description,
            url=self.url,
            state=self.state,
            avatar_url=self.avatar_url,
        )

    @classmethod
    def deserialize(
        cls,
        data: "QueueCheck.Serialized",
    ) -> "QueueCheck":
        return cls(
            name=data["name"],
            description=data["description"],
            url=data["url"],
            state=data["state"],
            avatar_url=data["avatar_url"],
        )


def get_check_list_ordered(
    checks: list[QueueCheck],
) -> list[QueueCheck]:
    checks_cpy = checks.copy()
    checks_cpy.sort(
        key=lambda c: (
            CHECK_SORTING.get(c.state, CHECK_SORTING["neutral"]),
            c.name,
            c.description,
        )
    )
    return checks_cpy
