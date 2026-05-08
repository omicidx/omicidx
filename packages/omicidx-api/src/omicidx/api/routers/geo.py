from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from omicidx.api.config import settings
from omicidx.api.db import get_session
from omicidx.api.models import GEOPlatform, GEOSample, GEOSeries
from omicidx.api.pagination import decode_cursor, encode_cursor
from omicidx.api.schemas.envelope import build_item_response, build_list_response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

Session = Annotated[AsyncSession, Depends(get_session)]


# -- Series --------------------------------------------------------------------


@router.get("/series/{accession}")
async def get_series(accession: str, session: Session):
    row = await session.get(GEOSeries, accession)
    if not row:
        raise HTTPException(404, detail=f"GEO series {accession} not found")
    return build_item_response(item=row.data)


@router.get("/series")
async def list_series(
    session: Session,
    organism: str | None = None,
    series_type: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    cursor: str | None = None,
    limit: Annotated[
        int, Query(ge=1, le=settings.max_page_size)
    ] = settings.default_page_size,
):
    stmt = select(GEOSeries).order_by(GEOSeries.accession).limit(limit + 1)

    if cursor:
        stmt = stmt.where(GEOSeries.accession > decode_cursor(cursor).after)
    if organism:
        stmt = stmt.where(GEOSeries.organism == organism)
    if series_type:
        stmt = stmt.where(GEOSeries.series_type == series_type)
    if date_from:
        stmt = stmt.where(GEOSeries.submission_date >= date_from)
    if date_to:
        stmt = stmt.where(GEOSeries.submission_date <= date_to)

    result = await session.execute(stmt)
    rows = result.scalars().all()
    has_next = len(rows) > limit
    items = [r.data for r in rows[:limit]]
    next_cursor = encode_cursor(rows[limit - 1].accession) if has_next else None

    return build_list_response(
        items=items,
        path="/v1/geo/series",
        limit=limit,
        next_cursor=next_cursor,
        cursor_param=cursor,
    )


# -- Samples -------------------------------------------------------------------


@router.get("/samples/{accession}")
async def get_sample(accession: str, session: Session):
    row = await session.get(GEOSample, accession)
    if not row:
        raise HTTPException(404, detail=f"GEO sample {accession} not found")

    relationships = {}
    if row.platform_id:
        relationships["platform"] = {
            "accession": row.platform_id,
            "href": f"/v1/geo/platforms/{row.platform_id}",
        }
    if row.series_id:
        relationships["series"] = {
            "accession": row.series_id,
            "href": f"/v1/geo/series/{row.series_id}",
        }

    return build_item_response(item=row.data, relationships=relationships or None)


@router.get("/samples")
async def list_samples(
    session: Session,
    organism: str | None = None,
    platform_id: str | None = None,
    cursor: str | None = None,
    limit: Annotated[
        int, Query(ge=1, le=settings.max_page_size)
    ] = settings.default_page_size,
):
    stmt = select(GEOSample).order_by(GEOSample.accession).limit(limit + 1)

    if cursor:
        stmt = stmt.where(GEOSample.accession > decode_cursor(cursor).after)
    if organism:
        stmt = stmt.where(GEOSample.organism == organism)
    if platform_id:
        stmt = stmt.where(GEOSample.platform_id == platform_id)

    result = await session.execute(stmt)
    rows = result.scalars().all()
    has_next = len(rows) > limit
    items = [r.data for r in rows[:limit]]
    next_cursor = encode_cursor(rows[limit - 1].accession) if has_next else None

    return build_list_response(
        items=items,
        path="/v1/geo/samples",
        limit=limit,
        next_cursor=next_cursor,
        cursor_param=cursor,
    )


# -- Platforms -----------------------------------------------------------------


@router.get("/platforms/{accession}")
async def get_platform(accession: str, session: Session):
    row = await session.get(GEOPlatform, accession)
    if not row:
        raise HTTPException(404, detail=f"GEO platform {accession} not found")
    return build_item_response(item=row.data)


@router.get("/platforms")
async def list_platforms(
    session: Session,
    organism: str | None = None,
    cursor: str | None = None,
    limit: Annotated[
        int, Query(ge=1, le=settings.max_page_size)
    ] = settings.default_page_size,
):
    stmt = select(GEOPlatform).order_by(GEOPlatform.accession).limit(limit + 1)

    if cursor:
        stmt = stmt.where(GEOPlatform.accession > decode_cursor(cursor).after)
    if organism:
        stmt = stmt.where(GEOPlatform.organism == organism)

    result = await session.execute(stmt)
    rows = result.scalars().all()
    has_next = len(rows) > limit
    items = [r.data for r in rows[:limit]]
    next_cursor = encode_cursor(rows[limit - 1].accession) if has_next else None

    return build_list_response(
        items=items,
        path="/v1/geo/platforms",
        limit=limit,
        next_cursor=next_cursor,
        cursor_param=cursor,
    )
