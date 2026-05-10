from typing import Any, Generic, TypeVar
from urllib.parse import urlencode

from pydantic import BaseModel

T = TypeVar("T")


class CursorInfo(BaseModel):
    next: str | None = None
    prev: str | None = None


class Meta(BaseModel):
    count: int
    cursor: CursorInfo | None = None


class Links(BaseModel):
    self: str
    next: str | None = None
    prev: str | None = None


class Relationship(BaseModel):
    accession: str | int
    href: str


class CollectionRelationship(BaseModel):
    """A relationship pointing at a paginated sub-resource collection."""

    href: str


class ListResponse(BaseModel, Generic[T]):
    data: list[T]
    meta: Meta
    links: Links


class ItemResponse(BaseModel, Generic[T]):
    data: T
    relationships: dict[str, Relationship | CollectionRelationship] | None = None


def build_list_response(
    *,
    items: list[dict[str, Any]],
    path: str,
    limit: int,
    next_cursor: str | None,
    prev_cursor: str | None = None,
    cursor_param: str | None = None,
    extra_params: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build a list envelope dict from query results.

    ``extra_params`` is merged into both ``links.self`` and ``links.next``
    so non-cursor query params (e.g. ``hydrate``) round-trip across pages.
    Values that are None are dropped.
    """
    base = path.split("?")[0]
    carry = {k: v for k, v in (extra_params or {}).items() if v is not None}

    def _qs(*pairs: tuple[str, str | int | None]) -> str:
        params = [(k, str(v)) for k, v in pairs if v is not None]
        params.extend(carry.items())
        return urlencode(params)

    next_link = (
        f"{base}?{_qs(('cursor', next_cursor), ('limit', limit))}"
        if next_cursor
        else None
    )
    self_link = f"{base}?{_qs(('cursor', cursor_param), ('limit', limit))}"

    return {
        "data": items,
        "meta": {
            "count": len(items),
            "cursor": {
                "next": next_cursor,
                "prev": prev_cursor,
            },
        },
        "links": {
            "self": self_link,
            "next": next_link,
            "prev": None,
        },
    }


def build_item_response(
    *,
    item: dict[str, Any],
    relationships: dict[str, Relationship | CollectionRelationship] | None = None,
) -> dict[str, Any]:
    """Build a single-item envelope dict."""
    result: dict[str, Any] = {"data": item}
    if relationships:
        result["relationships"] = {
            k: v.model_dump(exclude_none=True) for k, v in relationships.items()
        }
    return result
