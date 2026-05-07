from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from omicidx.api.config import settings
from omicidx.api.db import get_session
from omicidx.api.models import BioProject
from omicidx.api.pagination import decode_cursor, encode_cursor
from omicidx.api.schemas.envelope import build_item_response, build_list_response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

Session = Annotated[AsyncSession, Depends(get_session)]


@router.get("/{accession}")
async def get_bioproject(accession: str, session: Session):
    row = await session.get(BioProject, accession)
    if not row:
        raise HTTPException(404, detail=f"BioProject {accession} not found")
    return build_item_response(item=row.data)


@router.get("")
async def list_bioprojects(
    session: Session,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=settings.max_page_size)] = settings.default_page_size,
):
    stmt = select(BioProject).order_by(BioProject.accession).limit(limit + 1)

    if cursor:
        stmt = stmt.where(BioProject.accession > decode_cursor(cursor).after)

    result = await session.execute(stmt)
    rows = result.scalars().all()
    has_next = len(rows) > limit
    items = [r.data for r in rows[:limit]]
    next_cursor = encode_cursor(rows[limit - 1].accession) if has_next else None

    return build_list_response(
        items=items,
        path="/v1/bioproject",
        limit=limit,
        next_cursor=next_cursor,
        cursor_param=cursor,
    )
