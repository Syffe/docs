import dataclasses
import typing

import fastapi
import pydantic


DEFAULT_PER_PAGE = 10

T = typing.TypeVar("T")


@dataclasses.dataclass
class InvalidCursor(Exception):
    cursor: str


@dataclasses.dataclass
class _CurrentPage:
    request: fastapi.Request
    response: fastapi.Response
    cursor: str | None = None
    per_page: int = dataclasses.field(default=DEFAULT_PER_PAGE)


def get_current_page(
    request: fastapi.Request,
    response: fastapi.Response,
    cursor: typing.Annotated[
        str | None,
        fastapi.Query(
            description="The opaque cursor of the current page. Must be extracted for RFC 5988 pagination links to get first/previous/next/last pages",
        ),
    ] = None,
    per_page: typing.Annotated[
        int,
        fastapi.Query(
            ge=1,
            le=100,
            description="The number of items per page",
        ),
    ] = DEFAULT_PER_PAGE,
) -> "CurrentPage":
    return _CurrentPage(request, response, cursor, per_page)


CurrentPage = typing.Annotated[_CurrentPage, fastapi.Depends(get_current_page)]


@dataclasses.dataclass
class Page(typing.Generic[T]):
    items: list[T]
    current: CurrentPage
    total: int = dataclasses.field(default=0)
    cursor_prev: str | None = dataclasses.field(default=None)
    cursor_next: str | None = dataclasses.field(default=None)
    cursor_first: str = dataclasses.field(default="")
    cursor_last: str = dataclasses.field(default="-")

    @property
    def size(self) -> int:
        return len(self.items)


LinkHeader = {
    "Link": {
        "description": "Pagination links (rfc5988)",
        "example": """
Link: <https://api.mergify.com/v1/repos/Mergifyio/mergify-engine/events?cursor=def&per_page=20>; rel="next",
  <https://api.mergify.com/v1/repos/Mergifyio/mergify-engine/events?cursor=xyz&per_page=20>; rel="last",
  <https://api.mergify.com/v1/repos/Mergifyio/mergify-engine/events?cursor=abc&per_page=20>; rel="first",
  <https://api.mergify.com/v1/repos/Mergifyio/mergify-engine/events?cursor=abc&per_page=20>; rel="prev"
""".strip(),
        "schema": {"type": "string"},
    }
}


class PageResponse(typing.Generic[T], pydantic.BaseModel):
    # The attribute name under which all the items of the page will be stored
    items_key: typing.ClassVar[str]

    page: Page[T] = pydantic.Field(exclude=True)
    size: int = pydantic.Field(
        json_schema_extra={
            "metadata": {
                "description": "The number of items in this page",
            },
        }
    )
    per_page: int = pydantic.Field(
        json_schema_extra={
            "metadata": {
                "description": "The number of items per page",
            },
        }
    )
    total: int = pydantic.Field(
        json_schema_extra={
            "metadata": {
                "description": "The total number of items",
            },
        }
    )

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, page: Page[T]) -> None:
        page.current.response.headers["Link"] = self._build_link(page)
        kwargs = {
            "page": page,
            self.items_key: page.items,
            "size": page.size,
            "per_page": page.current.per_page,
            "total": page.total,
        }

        super().__init__(**kwargs)

    @staticmethod
    def _build_link(page: Page[T]) -> str:
        base_url = page.current.request.url

        links = {
            "first": base_url.include_query_params(
                per_page=page.current.per_page, cursor=page.cursor_first
            ),
            "last": base_url.include_query_params(
                per_page=page.current.per_page, cursor=page.cursor_last
            ),
        }
        if page.cursor_next is not None:
            links["next"] = base_url.include_query_params(
                per_page=page.current.per_page, cursor=page.cursor_next
            )
        if page.cursor_prev is not None:
            links["prev"] = base_url.include_query_params(
                per_page=page.current.per_page, cursor=page.cursor_prev
            )

        return ",".join([f'<{link}>; rel="{name}"' for name, link in links.items()])
