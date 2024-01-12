import dataclasses
import typing
import urllib.parse

import fastapi
import pydantic
import starlette

from mergify_engine.web import utils


DEFAULT_PER_PAGE = 10

T = typing.TypeVar("T")


@dataclasses.dataclass
class Cursor:
    value: str
    forward: bool

    @classmethod
    def from_string(cls, cursor: str) -> typing.Self:
        forward = not cursor.startswith("-")
        return cls(cursor.lstrip("+-"), forward)

    def to_string(self) -> str:
        return f"{'' if self.forward else '-'}{self.value}"

    @property
    def backward(self) -> bool:
        return not self.forward

    def next(
        self,
        first_item_id: str | None,
        last_item_id: str | None,
    ) -> typing.Self:
        if self.forward and last_item_id is not None:
            return self.__class__(last_item_id, forward=True)

        if self.backward and first_item_id is not None:
            return self.__class__(first_item_id, forward=False)

        return self.__class__("", forward=True)

    def previous(
        self,
        first_item_id: str | None,
        last_item_id: str | None,
    ) -> typing.Self:
        if self.forward and first_item_id is not None:
            return self.__class__(first_item_id, forward=False)

        if self.backward and last_item_id is not None:
            return self.__class__(last_item_id, forward=True)

        return self.__class__("", forward=False)


@dataclasses.dataclass
class InvalidCursorError(Exception):
    cursor: Cursor


@dataclasses.dataclass
class _CurrentPage:
    request: fastapi.Request
    response: fastapi.Response
    cursor: Cursor
    per_page: int = dataclasses.field(default=DEFAULT_PER_PAGE)


def get_current_page(
    request: fastapi.Request,
    response: fastapi.Response,
    cursor: typing.Annotated[
        str | None,
        fastapi.Query(
            description="The opaque cursor of the current page. Must be extracted for RFC 5988 pagination links to "
            "get first/previous/next/last pages",
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
    return _CurrentPage(request, response, Cursor.from_string(cursor or ""), per_page)


CurrentPage = typing.Annotated[_CurrentPage, fastapi.Depends(get_current_page)]


@dataclasses.dataclass
class Page(typing.Generic[T]):
    items: list[T]
    current: CurrentPage
    cursor_prev: Cursor | None = dataclasses.field(default=None)
    cursor_next: Cursor | None = dataclasses.field(default=None)
    cursor_first: Cursor = dataclasses.field(default_factory=lambda: Cursor("", True))
    cursor_last: Cursor = dataclasses.field(default_factory=lambda: Cursor("", False))

    @property
    def size(self) -> int:
        return len(self.items)


LinkHeader = {
    "Link": {
        "description": "Pagination links (rfc5988)",
        "example": """
Link: <https://api.mergify.com/v1/repos/Mergifyio/mergify-engine/logs?cursor=def&per_page=20>; rel="next",
  <https://api.mergify.com/v1/repos/Mergifyio/mergify-engine/logs?cursor=xyz&per_page=20>; rel="last",
  <https://api.mergify.com/v1/repos/Mergifyio/mergify-engine/logs?cursor=abc&per_page=20>; rel="first",
  <https://api.mergify.com/v1/repos/Mergifyio/mergify-engine/logs?cursor=abc&per_page=20>; rel="prev"
""".strip(),
        "schema": {"type": "string"},
    },
}


class PageResponse(pydantic.BaseModel, typing.Generic[T]):
    # The attribute name under which all the items of the page will be stored
    items_key: typing.ClassVar[str]

    page: Page[T] = pydantic.Field(exclude=True)
    size: int = pydantic.Field(
        json_schema_extra={
            "metadata": {
                "description": "The number of items in this page",
            },
        },
    )
    per_page: int = pydantic.Field(
        json_schema_extra={
            "metadata": {
                "description": "The number of items per page",
            },
        },
    )

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    def __init__(
        self,
        page: Page[T],
        query_parameters: dict[str, typing.Any] | None = None,
    ) -> None:
        page.current.response.headers["Link"] = self._build_link(page, query_parameters)
        kwargs = {
            "page": page,
            self.items_key: page.items,
            "size": page.size,
            "per_page": page.current.per_page,
        }

        super().__init__(**kwargs)

    @staticmethod
    def _build_link(
        page: Page[T],
        query_parameters: dict[str, typing.Any] | None,
    ) -> str:
        base_url = page.current.request.url

        if query_parameters is not None:
            cleaned_query_parameters = utils.serialize_query_parameters(
                query_parameters,
            )
        else:
            cleaned_query_parameters = {}

        links_query_parameters = {
            "first": {
                "per_page": page.current.per_page,
                "cursor": page.cursor_first.to_string(),
                **cleaned_query_parameters,
            },
            "last": {
                "per_page": page.current.per_page,
                "cursor": page.cursor_last.to_string(),
                **cleaned_query_parameters,
            },
        }

        if page.cursor_next is not None:
            links_query_parameters["next"] = {
                "per_page": page.current.per_page,
                "cursor": page.cursor_next.to_string(),
                **cleaned_query_parameters,
            }
        if page.cursor_prev is not None:
            links_query_parameters["prev"] = {
                "per_page": page.current.per_page,
                "cursor": page.cursor_prev.to_string(),
                **cleaned_query_parameters,
            }
        links = {
            name: base_url.replace(query=urllib.parse.urlencode(qp, doseq=True))
            for name, qp in links_query_parameters.items()
        }

        return generate_links_header(links)


def generate_links_header(
    links: dict[str, starlette.datastructures.URL],
) -> str:
    return ",".join([f'<{link}>; rel="{name}"' for name, link in links.items()])
