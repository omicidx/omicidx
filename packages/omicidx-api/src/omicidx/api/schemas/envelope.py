from typing import Any, Generic, TypeVar

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


class ListResponse(BaseModel, Generic[T]):
    data: list[T]
    meta: Meta
    links: Links


class ItemResponse(BaseModel, Generic[T]):
    data: T
    relationships: dict[str, Relationship] | None = None


def build_list_response(
    *,
    items: list[dict[str, Any]],
    path: str,
    limit: int,
    next_cursor: str | None,
    prev_cursor: str | None = None,
    cursor_param: str | None = None,
) -> dict[str, Any]:
    """Build a list envelope dict from query results."""
    base = path.split("?")[0]

    next_link = f"{base}?cursor={next_cursor}&limit={limit}" if next_cursor else None
    self_link = f"{base}?cursor={cursor_param}&limit={limit}" if cursor_param else f"{base}?limit={limit}"

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
    relationships: dict[str, Relationship] | None = None,
) -> dict[str, Any]:
    """Build a single-item envelope dict."""
    result: dict[str, Any] = {"data": item}
    if relationships:
        result["relationships"] = {
            k: v.model_dump() for k, v in relationships.items()
        }
    return result
