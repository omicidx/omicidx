from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from omicidx.api.config import settings
from omicidx.api.db import get_session
from omicidx.api.models import BioSample
from omicidx.api.pagination import decode_cursor, encode_cursor
from omicidx.api.schemas.envelope import (
    Relationship,
    build_item_response,
    build_list_response,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

Session = Annotated[AsyncSession, Depends(get_session)]


@router.get("/{accession}")
async def get_biosample(accession: str, session: Session):
    row = await session.get(BioSample, accession)
    if not row:
        raise HTTPException(404, detail=f"BioSample {accession} not found")

    relationships = {}
    if row.sra_sample_id:
        relationships["sra_sample"] = Relationship(
            accession=row.sra_sample_id,
            href=f"/v1/sra/samples/{row.sra_sample_id}",
        )

    return build_item_response(item=row.data, relationships=relationships or None)


@router.get("")
async def list_biosamples(
    session: Session,
    organism: str | None = None,
    tax_id: int | None = None,
    is_reference: bool | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    cursor: str | None = None,
    limit: Annotated[
        int, Query(ge=1, le=settings.max_page_size)
    ] = settings.default_page_size,
):
    stmt = select(BioSample).order_by(BioSample.accession).limit(limit + 1)

    if cursor:
        page = decode_cursor(cursor)
        stmt = stmt.where(BioSample.accession > page.after)
    if organism:
        stmt = stmt.where(BioSample.organism == organism)
    if tax_id is not None:
        stmt = stmt.where(BioSample.tax_id == tax_id)
    if is_reference is not None:
        stmt = stmt.where(BioSample.is_reference == is_reference)
    if date_from:
        stmt = stmt.where(BioSample.submission_date >= date_from)
    if date_to:
        stmt = stmt.where(BioSample.submission_date <= date_to)

    result = await session.execute(stmt)
    rows = result.scalars().all()

    has_next = len(rows) > limit
    items = [r.data for r in rows[:limit]]
    next_cursor = encode_cursor(rows[limit - 1].accession) if has_next else None

    return build_list_response(
        items=items,
        path="/v1/biosample",
        limit=limit,
        next_cursor=next_cursor,
        cursor_param=cursor,
    )
