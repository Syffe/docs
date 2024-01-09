import dataclasses
import typing
import urllib.parse

import fastapi
import pydantic
import pydantic_core

from mergify_engine.web import utils


DEFAULT_PER_PAGE = 10

T = typing.TypeVar("T")


# FIXME(charly): it has to inherit from `str` as the pull request endpoint
# doesn't fully implement cursor and should be refactored.
class Cursor(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: typing.Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        return pydantic_core.core_schema.no_info_after_validator_function(
            cls,
            handler(str),
        )

    @property
    def forward(self) -> bool:
        return not self or self.startswith("+")

    @property
    def backward(self) -> bool:
        return self.startswith("-")

    @property
    def value(self) -> str:
        return self.lstrip("+-")

    def next(
        self,
        first_item_id: object | None,
        last_item_id: object | None,
    ) -> "Cursor":
        if self.forward and last_item_id is not None:
            return self.__class__(f"+{last_item_id}")

        if self.backward and first_item_id is not None:
            return self.__class__(f"-{first_item_id}")

        return self.__class__("")

    def previous(
        self,
        first_item_id: object | None,
        last_item_id: object | None,
    ) -> "Cursor":
        if self.forward and first_item_id is not None:
            return self.__class__(f"-{first_item_id}" if self else "")

        if self.backward and last_item_id is not None:
            return self.__class__(f"+{last_item_id}" if self != "-" else "")

        return self.__class__("")


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
    return _CurrentPage(request, response, Cursor(cursor or ""), per_page)


CurrentPage = typing.Annotated[_CurrentPage, fastapi.Depends(get_current_page)]


@dataclasses.dataclass
class Page(typing.Generic[T]):
    items: list[T]
    current: CurrentPage
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
                "cursor": page.cursor_first,
                **cleaned_query_parameters,
            },
            "last": {
                "per_page": page.current.per_page,
                "cursor": page.cursor_last,
                **cleaned_query_parameters,
            },
        }

        if page.cursor_next is not None:
            links_query_parameters["next"] = {
                "per_page": page.current.per_page,
                "cursor": page.cursor_next,
                **cleaned_query_parameters,
            }
        if page.cursor_prev is not None:
            links_query_parameters["prev"] = {
                "per_page": page.current.per_page,
                "cursor": page.cursor_prev,
                **cleaned_query_parameters,
            }

        links = {
            name: base_url.replace(query=urllib.parse.urlencode(qp, doseq=True))
            for name, qp in links_query_parameters.items()
        }

        return ",".join([f'<{link}>; rel="{name}"' for name, link in links.items()])
