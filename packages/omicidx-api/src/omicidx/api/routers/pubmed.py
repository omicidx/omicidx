from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from omicidx.api.config import settings
from omicidx.api.db import get_session
from omicidx.api.models import PubMedArticle
from omicidx.api.pagination import decode_cursor, encode_cursor
from omicidx.api.schemas.envelope import build_item_response, build_list_response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

Session = Annotated[AsyncSession, Depends(get_session)]


@router.get("/{pmid}")
async def get_article(pmid: int, session: Session):
    row = await session.get(PubMedArticle, pmid)
    if not row:
        raise HTTPException(404, detail=f"PubMed article {pmid} not found")
    return build_item_response(item=row.data)


@router.get("")
async def list_articles(
    session: Session,
    journal: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=settings.max_page_size)] = settings.default_page_size,
):
    stmt = select(PubMedArticle).order_by(PubMedArticle.pmid).limit(limit + 1)

    if cursor:
        stmt = stmt.where(PubMedArticle.pmid > decode_cursor(cursor).after)
    if journal:
        stmt = stmt.where(PubMedArticle.journal == journal)
    if date_from:
        stmt = stmt.where(PubMedArticle.pub_date >= date_from)
    if date_to:
        stmt = stmt.where(PubMedArticle.pub_date <= date_to)

    result = await session.execute(stmt)
    rows = result.scalars().all()
    has_next = len(rows) > limit
    items = [r.data for r in rows[:limit]]
    next_cursor = encode_cursor(rows[limit - 1].pmid) if has_next else None

    return build_list_response(
        items=items, path="/v1/pubmed", limit=limit,
        next_cursor=next_cursor, cursor_param=cursor,
    )
